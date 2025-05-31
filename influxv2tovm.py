#!/usr/bin/env python3
"""
 @author Fredrik Lilja
 modified by Pbranly

 SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
import warnings
from typing import Iterable, Dict, List, Optional
from datetime import datetime, timedelta

import humanize
import pandas as pd
import requests
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)

logger = logging.getLogger(__name__)
logging.basicConfig(filename="migrator.log", encoding="utf-8", level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s -  %(message)s')

try:
    import dotenv
    dotenv.load_dotenv(dotenv_path=".env")
except ImportError:
    pass


class Stats:
    bytes: int = 0
    lines: int = 0

    def humanized_bytes(self) -> str:
        return humanize.naturalsize(self.bytes)

    def increment(self, lines: str):
        no_lines = lines.count('\n') + 1 if lines else 0
        self.lines += no_lines
        new_bytes = len(lines.encode("utf8")) if lines else 0
        self.bytes += new_bytes


class InfluxMigrator:
    __query_api: QueryApi
    __measurement_key = "_measurement"
    __client: InfluxDBClient

    def __init__(self, bucket: str, vm_url: str, chunksize: int = 100, dry_run: bool = False, pivot: bool = False):
        self.bucket = bucket
        self.vm_url = vm_url
        self.chunksize = chunksize
        self.stats = Stats()
        self.dry_run = dry_run
        self.pivot = pivot
        if pivot:
            self.__measurement_key = "entity_id"
        else:
            self.__measurement_key = "_measurement"

    def __del__(self):
        if hasattr(self, '__client'):
            self.__client.close()

    def influx_connect(self):
        self.__client = InfluxDBClient(
            url=os.getenv("INFLUXDB_V2_URL"),
            token=os.getenv("INFLUXDB_V2_TOKEN"),
            org=os.getenv("INFLUXDB_V2_ORG"),
            timeout=60000
        )
        self.__query_api = self.__client.query_api()

    def migrate(self, start_date: Optional[datetime.date] = None, end_date: Optional[datetime.date] = None):
        if self.__query_api is None:
            raise AssertionError("No connection to InfluxDb started.")

        measurements = self.__find_all_measurements()

        field_no = 1
        for meas in measurements:
            no_lines = 0
            offset = 0
            df_empty = False

            while not df_empty:
                date_range = ""
                if start_date and end_date:
                    start_iso = datetime.combine(start_date, datetime.min.time()).isoformat() + "Z"
                    stop_iso = (datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)).isoformat() + "Z"
                    date_range = f'|> range(start: {start_iso}, stop: {stop_iso})'
                elif start_date:
                    start_iso = datetime.combine(start_date, datetime.min.time()).isoformat() + "Z"
                    date_range = f'|> range(start: {start_iso})'
                else:
                    date_range = '|> range(start: -100d, stop: now())'

                chunk_query = f'''
                    from(bucket: "{self.bucket}")
                    {date_range}
                    |> filter(fn: (r) => r["{self.__measurement_key}"] == "{meas}")
                    |> limit(n: {self.chunksize}, offset: _offset)
                '''

                params = {"_offset": offset}
                result = self.__query_api.query_data_frame(chunk_query, params=params)
                if not isinstance(result, list):
                    result = [result]

                for df in result:
                    df_empty = df.empty
                    if df_empty:
                        break

                    offset += df.shape[0]

                    lines_protocol_str = self.__get_influxdb_lines(df)
                    if lines_protocol_str:
                        self.stats.increment(lines_protocol_str)
                        no_lines += lines_protocol_str.count('\n') + 1

                        if not self.dry_run:
                            self.__send_lines_in_chunks(lines_protocol_str, chunk_size=10000)
                        else:
                            print(lines_protocol_str + "\n")

                    print(
                        f"Wrote {no_lines} lines "
                        f"bytes to VictoriaMetrics db={self.bucket} for {meas}. "
                        f"Total: {self.stats.humanized_bytes()} "
                        f"({field_no}/{len(measurements)})",
                        end='\r',
                        flush=True
                    )
            field_no += 1

        print("\nAll done")

    def __send_lines_in_chunks(self, lines_protocol_str: str, chunk_size: int = 500):
        lines = lines_protocol_str.split('\n')
        for i in range(0, len(lines), chunk_size):
            chunk = "\n".join(lines[i:i + chunk_size])
            r = requests.post(
                f"{self.vm_url}/write?db={self.bucket}",
                data=chunk.encode("utf8")
            )
            if r.status_code != 204:
                print(f"❌ Erreur POST VM: {r.status_code}, texte: {r.text}\n")

    def __find_all_measurements(self):
        print("Finding unique time series.\n")
        first_in_series = f"""
           from(bucket: "{self.bucket}")
           |> range(start: 0, stop: now())
           |> first()"""
        timeseries: List[pd.DataFrame] = self.__query_api.query_data_frame(first_in_series)

        if isinstance(timeseries, pd.DataFrame):
            timeseries = [timeseries]
        elif not isinstance(timeseries, list):
            print(f"Unexpected result type: {type(timeseries)}\n")
            exit(500)

        measurements = set()
        for df in timeseries:
            if self.__measurement_key in df.columns:
                measurements.update(df[self.__measurement_key].dropna().unique())
            else:
                print(f"⚠️ Colonne '{self.__measurement_key}' absente, mesure ignorée.\n")

        print(f"Found {len(measurements)} unique time series\n")
        return measurements

    @staticmethod
    def __get_tag_cols(dataframe_keys: Iterable) -> Iterable:
        return (
            k
            for k in dataframe_keys
            if not k.startswith("_") and k not in ["result", "table"]
        )

    @staticmethod
    def escape_influx_string(x: str) -> str:
        return x.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')

    def __get_influxdb_lines(self, df: pd.DataFrame) -> str:
        logger.info(f"Exporting {df.columns}")

        if df.empty:
            logger.debug(f"No data points for this")
            return ""

        lines = []

        for i in range(len(df)):
            if self.pivot:
                measurement = str(df["domain"].iloc[i]) + "." + str(df["entity_id"].iloc[i])
            else:
                measurement = str(df["_measurement"].iloc[i])

            # Build tags
            tags = []
            for col_name in self.__get_tag_cols(df.columns):
                val = df[col_name].iloc[i]
                if pd.isna(val) or val == "":
                    continue
                sval = str(val).strip()
                if sval == "" or sval.lower() == "nan":
                    continue
                esc_key = col_name.replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")
                esc_val = sval.replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")
                tags.append(f"{esc_key}={esc_val}")
            if self.pivot:
                uom = df["_measurement"].iloc[i]
                if pd.notna(uom) and str(uom).strip() != "":
                    uom_str = str(uom).replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")
                    tags.append(f"unit_of_measurement={uom_str}")

            tag_str = "," + ",".join(tags) if tags else ""

            # Field key and value
            field_key = df["_field"].iloc[i]
            field_val_raw = df["_value"].iloc[i]
            if pd.isna(field_val_raw) or field_val_raw == "" or field_val_raw is None:
                continue

            esc_field_key = field_key.replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")
            if isinstance(field_val_raw, str):
                esc_field_val = f'"{self.escape_influx_string(field_val_raw)}"'
            elif isinstance(field_val_raw, bool):
                esc_field_val = "true" if field_val_raw else "false"
            else:
                esc_field_val = str(field_val_raw)

            ts = int(pd.to_datetime(df["_time"].iloc[i]).timestamp() * 1e9)

            line = f"{measurement}{tag_str} {esc_field_key}={esc_field_val} {ts}"
            lines.append(line)

        return "\n".join(lines)


def main(args: Dict[str, str]):
    env_vars = ["INFLUXDB_V2_ORG", "INFLUXDB_V2_URL", "INFLUXDB_V2_TOKEN", "VM_ADDR"]
    for var in env_vars:
        if args.get(var) is not None:
            os.environ[var] = args[var]

    bucket = args.get("bucket")
    vm_url = args.get("VM_ADDR") or os.environ.get("VM_ADDR")
    dry_run = bool(args.get("dry_run"))
    pivot = bool(args.get("pivot"))

    migrator = InfluxMigrator(bucket, vm_url, chunksize=5000, dry_run=dry_run, pivot=pivot)
    migrator.influx_connect()

    start_date = None
    end_date = None
    if "start" in args and args["start"]:
        try:
            start_date = datetime.strptime(args["start"], "%Y-%m-%d").date()
        except ValueError:
            print(f"Erreur: format de date start invalide : {args['start']}. Utilisez YYYY-MM-DD.\n")
            exit(1)

    if "end" in args and args["end"]:
        try:
            end_date = datetime.strptime(args["end"], "%Y-%m-%d").date()
        except ValueError:
            print(f"Erreur: format de date end invalide : {args['end']}. Utilisez YYYY-MM-DD.\n")
            exit(1)

    migrator.migrate(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into VictoriaMetrics instance.\n"
                    "InfluxDB settings can be définies en ligne de commande ou via variables d'environnement."
    )
    parser.add_argument("bucket", type=str, help="InfluxDB source bucket")
    parser.add_argument("--INFLUXDB_V2_ORG", type=str, help="InfluxDB organization")
    parser.add_argument("--INFLUXDB_V2_URL", type=str, help="InfluxDB Server URL, e.g., http://localhost:8086")
    parser.add_argument("--INFLUXDB_V2_TOKEN", type=str, help="InfluxDB API token")
    parser.add_argument("--VM_ADDR", type=str, help="VictoriaMetrics server address, e.g., http://localhost:8428")
    parser.add_argument("--start", type=str, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date in YYYY-MM-DD")
    parser.add_argument("--dry_run", action="store_true", help="Print output only, no POST")
    parser.add_argument("--pivot", action="store_true", help="Use pivot mode")

    args = vars(parser.parse_args())

    main(args)
