import json
from io import BytesIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import datetime

#import numpy as np


# Constants
AWS_BUCKET = "bdi-aircraft-myrthe"
AWS_CONN_ID = "aws_default"
RAW_PREFIX_TEMPLATE = "raw/day={}/"
PREP_PREFIX_TEMPLATE = "prepare/day={}/prepared_data.csv"

@dag(
    dag_id="prepare_readsb_data_dag",
    description="Prepare readsb JSON.gz files and store clean CSVs in S3 (PREP)",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    schedule="@monthly",
    catchup=True,
    tags=["readsb", "prepare", "clean", "csv"],
    is_paused_upon_creation=False,
)
def prepare_readsb_data():

    @task()
    def process_day(execution_date=None):
        if execution_date is None:
            raise ValueError("execution_date is required")

        date_str = execution_date.strftime("%Y%m%d")
        raw_prefix = RAW_PREFIX_TEMPLATE.format(date_str)
        prep_key = PREP_PREFIX_TEMPLATE.format(date_str)

        # Columns to retain
        columns_to_keep = ['hex', 'type', 'r', 't', 'alt_baro', 'gs', 'emergency', 'lat', 'lon', 'timestamp']
        emergency_types = {'general', 'lifeguard', 'minfuel', 'reserved', 'nordo', 'unlawful', 'downed'}

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3 = hook.get_conn()

        # Check if file already prepared
        try:
            s3.head_object(Bucket=AWS_BUCKET, Key=prep_key)
            print(f"Prepared file {prep_key} already exists. Skipping.")
            return
        except s3.exceptions.ClientError:
            pass

        # List raw JSON files
        objects = hook.list_keys(bucket_name=AWS_BUCKET, prefix=raw_prefix)
        if not objects:
            print(f"No raw data found for {raw_prefix}")
            return

        all_data = []

        for key in objects:
            if not key.endswith(".json.gz"):
                continue
            obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
            content = obj["Body"].read()
            data = json.loads(content)

            timestamp = data.get("now")

            if "aircraft" not in data:
                continue

            df = pd.DataFrame(data["aircraft"])
            df["timestamp"] = timestamp
            df = df[[col for col in columns_to_keep if col in df.columns]]

            df.rename(
                columns={"hex": "icao", "r": "registration", "t": "icao_type",
                         "alt_baro": "altitude_baro", "gs": "ground_speed"},
                inplace=True
            )

            df['altitude_baro'] = df['altitude_baro'].replace('ground', '0')

            df = df.astype(str)
            df = df[~df["icao"].str.startswith("~")]

            if "emergency" in df.columns:
                df["had_emergency"] = df["emergency"].apply(lambda x: x in emergency_types)
                df.drop(columns=["emergency"], inplace=True)

            df.dropna(subset=["icao", "registration", "icao_type", "lat", "lon"], inplace=True)
            df.drop_duplicates(subset=["icao", "timestamp"], inplace=True)

            all_data.append(df)

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            buffer = BytesIO()
            final_df.to_csv(buffer, index=False)
            buffer.seek(0)

            s3.upload_fileobj(buffer, AWS_BUCKET, prep_key)
            print(f"Uploaded cleaned file to {prep_key}")
        else:
            print(f"No valid aircraft data found for {raw_prefix}")

    process_day()

dag = prepare_readsb_data()
