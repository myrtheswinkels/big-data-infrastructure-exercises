from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = 'bdi-aircraft-myrthe'
S3_KEY = 'aircraft_data/aircraft_type_fuel_consumption_rates.json'
URL = 'https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json'
AWS_CONN_ID = 'aws_default'

@task
def download_and_upload_to_s3():
    response = requests.get(URL)
    response.raise_for_status()
    data = response.content

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_bytes(
        bytes_data=data,
        key=S3_KEY,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Uploaded {S3_KEY} to {S3_BUCKET}")

@dag(
    dag_id='download_aircraft_fuel_consumption_dag',
    start_date=datetime(2023, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=['project', 'aircraft', 'fuel'],
    is_paused_upon_creation=False
)
def aircraft_fuel_dag():
    download_and_upload_to_s3()

dag = aircraft_fuel_dag()
