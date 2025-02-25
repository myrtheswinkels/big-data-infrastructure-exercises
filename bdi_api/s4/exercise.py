import concurrent.futures
import json
import os
import shutil
from typing import Annotated

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""Limits the number of files to download.
            You must always start from the first the page returns and
            go in ascending order in order to correctly obtain the results.
            I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Download aircraft data and upload it to AWS S3 in parallel."""
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix = "data/20231101/"

    # Clean and recreate the download directory
    os.makedirs(download_dir, exist_ok=True)


    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    file_links = [i["href"] for i in soup.find_all("a") 
                  if i["href"].endswith(".json.gz")][:file_limit]

    
    s3_client = boto3.client("s3")

    def process_file(href):
        """Download file and upload to S3."""
        try:
            file_url = os.path.join(base_url, href)
            local_file_path = os.path.join(download_dir, href[:-3])  
            s3_file_path = os.path.join(s3_prefix, href[:-3])  
            # Stream file download
            file_response = requests.get(file_url, stream=True)
            file_response.raise_for_status()
            with open(local_file_path, "wb") as f:
                for chunk in file_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Upload to S3
            s3_client.upload_file(local_file_path, s3_bucket, s3_file_path)

        except Exception as e:
            print(f"Failed to process {href}: {e}")

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_file, file_links)

    return f"Files downloaded and uploaded to s3://{s3_bucket}/{s3_prefix}"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    s3_bucket = settings.s3_bucket
    s3_prefix = "data/20231101/"
    prepared_s3_prefix = "prepared/20231101/"

    # Clean and recreate the prepared directory
    if os.path.exists(prep_dir):
        shutil.rmtree(prep_dir)
    os.makedirs(prep_dir, exist_ok=True)

    # Initialize S3 client
    s3_client = boto3.client("s3")

    # List objects in the S3 bucket
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    if "Contents" not in response:
        return "No files found in S3 bucket."

    all_data = []
    columns_to_keep = ['hex', 'type', 'r', 't', 'alt_baro', 'gs', 'emergency', 'lat', 'lon', 'timestamp']
    emergency_types = {'general', 'lifeguard', 'minfuel', 'reserved', 'nordo', 'unlawful', 'downed'}

    for obj in response["Contents"]:
        s3_file_key = obj["Key"]
        local_file_path = os.path.join(raw_dir, os.path.basename(s3_file_key))

        # Download file from S3
        s3_client.download_file(s3_bucket, s3_file_key, local_file_path)

        with open(local_file_path, "rb") as f:
            data = json.load(f)

        timestamp = data.get("now")

        if "aircraft" in data:
            df = pd.DataFrame(data["aircraft"])
            df["timestamp"] = timestamp
            df = df[[col for col in columns_to_keep if col in df.columns]]

            df.rename(
                columns={"hex": "icao", "r": "registration", "t": "type", "alt_baro": "altitude_baro", "gs": "ground_speed"},
                inplace=True
            )

            df = df.astype(str)
            df = df[~df["icao"].str.startswith("~")]

            if "emergency" in df.columns:
                df["had_emergency"] = df["emergency"].apply(lambda x: x in emergency_types)
                df.drop(columns=["emergency"], inplace=True)

            df.dropna(subset=["icao", "registration", "type", "lat", "lon"], inplace=True)
            df.drop_duplicates(subset=["icao", "timestamp"], inplace=True)

            all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        prepared_file_path = os.path.join(prep_dir, "prepared_data.csv")
        final_df.to_csv(prepared_file_path, index=False)

    s3_client.upload_file(prepared_file_path, s3_bucket, os.path.join(prepared_s3_prefix, "prepared_data.csv"))

    return "OK"
