import os
from io import BytesIO
from urllib.parse import urljoin

import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import datetime
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/"
AWS_BUCKET = "bdi-aircraft-myrthe"
AWS_CONN_ID = "aws_default"

@dag(
    dag_id="download_readsb_data",
    description="Download 100 readsb JSON.gz files and upload to S3 (RAW)",
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=True,
    tags=["readsb", "download", "raw"],
    params={"limit": Param(100, type="integer", description="File limit per day")},
    is_paused_upon_creation=False,
)
def download_readsb_data():

    @task()
    def download_for_day(execution_date=None, limit: int = 100):
        if execution_date is None:
            raise ValueError("execution_date is required")

        day_str = execution_date.strftime("%Y/%m/%d")
        s3_prefix = f"raw/day={execution_date.strftime('%Y%m%d')}/"
        url = urljoin(BASE_URL, day_str + "/")

        print(f"Fetching file list from {url}")
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a")
            if a["href"].endswith(".json.gz")
        ][:limit]

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_client = hook.get_conn()

        for filename in files:
            s3_key = os.path.join(s3_prefix, filename).replace("\\", "/")
            file_url = urljoin(url, filename)

            try:
                s3_client.head_object(Bucket=AWS_BUCKET, Key=s3_key)
                print(f"File {s3_key} already exists in S3. Skipping.")
                continue
            except s3_client.exceptions.ClientError:
                pass

            print(f"Downloading {file_url}")
            r = requests.get(file_url, stream=True)
            if r.status_code == 200:
                s3_client.upload_fileobj(BytesIO(r.content), AWS_BUCKET, s3_key)
                print(f"Uploaded {s3_key} to S3")

    download_for_day()

dag = download_readsb_data()
