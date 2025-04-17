from io import BytesIO

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import datetime

# Constants
DATABASE_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
AWS_BUCKET = "bdi-aircraft-myrthe"
AWS_CONN_ID = "aws_default"
S3_KEY = "aircraft_database/AircraftDatabase.zip"

@dag(
    dag_id="download_aircraft_database_dag",
    description="Download the Aircraft Database and upload to S3 (full refresh)",
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
    tags=["aircraft", "database", "download", "raw"],
    is_paused_upon_creation=False,
)
def download_aircraft_database():

    @task()
    def download_and_upload():
        print(f"Downloading Aircraft Database from {DATABASE_URL}")
        response = requests.get(DATABASE_URL, stream=True)
        response.raise_for_status()

        # Get S3 client using S3Hook
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_client = hook.get_conn()

        print(f"Uploading to s3://{AWS_BUCKET}/{S3_KEY} (overwrite)")
        s3_client.upload_fileobj(BytesIO(response.content), AWS_BUCKET, S3_KEY)
        print("Upload completed.")

    download_and_upload()

dag = download_aircraft_database()
