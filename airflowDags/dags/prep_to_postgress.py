from io import BytesIO

import duckdb
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import datetime

#import numpy as np


AWS_BUCKET = "bdi-aircraft-myrthe"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_aircraft"

@dag(
    dag_id="load_prepared_data_with_duckdb_to_postgres",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    schedule="@monthly",
    catchup=True,
    tags=["duckdb", "postgres", "load", "prepared"],
    is_paused_upon_creation=False
)
def load_prepared_data():
    def create_table(engine): 
        with engine.connect() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_data (
                icao VARCHAR,
                type VARCHAR,
                registration VARCHAR,
                icao_type VARCHAR,
                altitude_baro FLOAT,
                ground_speed FLOAT,
                lat FLOAT,
                lon FLOAT,
                timestamp FLOAT,
                had_emergency BOOLEAN
            );
            """)
        

    @task()
    def process_and_upload(execution_date=None):
        if execution_date is None:
            raise ValueError("execution_date is required")

        date_str = execution_date.strftime("%Y%m%d")
        s3_key = f"prep/day={date_str}/prepared_data.csv"

        # Download CSV from S3
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3 = s3_hook.get_conn()
        obj = s3.get_object(Bucket=AWS_BUCKET, Key=s3_key)
        df_raw = pd.read_csv(BytesIO(obj['Body'].read()))

        # Use DuckDB for SQL processing
        con = duckdb.connect()
        con.register("aircraft_data", df_raw)

        # SQL processing
        df_processed = con.execute("SELECT * FROM aircraft_data").df()
        con.close()
        df_processed['altitude_baro'] = df_processed['altitude_baro'].replace('ground', '0')
        # Upload to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        create_table(engine)
        # Check if table exists and create if not
        df_processed.to_sql("aircraft_data", engine, if_exists="append", index=False)

        print(f"Loaded {len(df_processed)} rows from {s3_key} into PostgreSQL table 'aircraft_data'.")

    process_and_upload()

dag = load_prepared_data()
