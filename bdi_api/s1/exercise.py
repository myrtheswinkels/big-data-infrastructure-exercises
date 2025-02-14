import json
import os
import shutil
from typing import Annotated

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, Query, status

from bdi_api.settings import Settings

settings = Settings()
s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    #Check if the download directory exists, if it does, delete it
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    #This first step is to get a list of all the files that are in the directory. This will delete the exiting files
    #in the directory so that I can download the new ones.

    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    #I used BeutifulSoup to parse the html file and get the links to the files that I need to download.

    #Get the links to the files that I need to download
    file_links = [
        i["href"] for i in soup.find_all("a") if i["href"].endswith(".json.gz")
    ][:file_limit]

    for href in file_links:
        file_url = os.path.join(base_url, href)
        local_file_path = os.path.join(download_dir, href[:-3])
        #I used href[:-3] to remove the .gz extension from the file name. So now the file name will be a json file.

        file_response = requests.get(file_url)
        file_response.raise_for_status()

        #Write the file to the local directory and save it as a json file
        with open(local_file_path, "wb") as f:
            f.write(file_response.content)

    return "Files are downloaded"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")

    if os.path.exists(prep_dir):
        shutil.rmtree(prep_dir)
    os.makedirs(prep_dir, exist_ok=True)
    #This first step is to get a list of all the files that are in the prep directory.This will delete the exiting files
    #in the directory so that I can add the new ones.

    columns_to_keep = ['hex', 'type', 'r', 't', 'alt_baro', 'gs', 'emergency', 'lat', 'lon', 'timestamp']
    #These are the columns that I want to keep in the data, these are used in the following exercises.
    #I will drop the columns that are not in this list.
    emergency_types = {'general', 'lifeguard', 'minfuel', 'reserved', 'nordo', 'unlawful', 'downed'}
    #These are the emergency types that I will use in the data, except 'none'.
    #I will use this to check if the emergency column has any of these values.

    all_data = []

    for file in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file)
        with open(file_path, "rb") as f:
            data = json.load(f)
            #I used the json.load method to load the json file and store it in the data variable.

        timestamp = data.get("now")
        #I figured out that the timestamp is in the now key of the json file.
        #I stored this in the timestamp variable.
        if "aircraft" in data:
            df = pd.DataFrame(data["aircraft"])
            df["timestamp"] = timestamp
            df = df[[col for col in columns_to_keep if col in df.columns]]

            df.rename(
                columns={"hex":"icao","r":"registration","t": "type","alt_baro":"altitude_baro","gs":"ground_speed"},
                inplace=True
            )

            df = df.astype(str)
            df = df[~df["icao"].str.startswith("~")]
            #I dropped the rows that have the icao column starting with ~, because these are not valid icao numbers.

            if "emergency" in df.columns:
                df["had_emergency"] = df["emergency"].apply(lambda x: x in emergency_types)
                df.drop(columns=["emergency"], inplace=True)
                #I added a new column called had_emergency, which is True if the emergency column has any of 
                #the emergency types that I defined above.

            df.dropna(subset=["icao", "registration", "type", "lat", "lon"], inplace=True)
            df.drop_duplicates(subset=["icao", "timestamp"], inplace=True)
            #I dropped the rows that have missing values in the icao, registration, type, lat, and lon columns.
            #I also dropped the duplicates in the icao and timestamp columns.

            all_data.append(df)
            #I appended the dataframes to the all_data list.

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(os.path.join(prep_dir, "prepared_data.csv"), index=False)
        #I concatenated all the dataframes in the all_data list and saved the final dataframe as a csv file
        #in the prep directory.

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prep_dir, "prepared_data.csv")

    if not os.path.exists(file_path):
        return []

    df = pd.read_csv(file_path, usecols=["icao", "registration", "type"]).drop_duplicates()
    df.sort_values(by="icao", ascending=True, inplace=True)
    #I read the csv file and dropped the duplicates in the icao column. I sorted the dataframe by the icao column in
    #ascending order.

    start, end = page * num_results, (page + 1) * num_results
    #I calculated the start and end indices for the page.
    return df.iloc[start:end].to_dict(orient="records")
    #I returned the data for the page as a list of dictionaries.


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prep_dir, "prepared_data.csv")

    if not os.path.exists(file_path):
        return []

    df = pd.read_csv(file_path, usecols=["icao", "timestamp", "lat", "lon"])
    df = df[df["icao"] == icao].sort_values(by="timestamp")
    #I read the csv file and selected the icao, timestamp, lat, and lon columns.
    #I filtered the dataframe for the given icao and sorted the dataframe by the timestamp column.

    start, end = page * num_results, (page + 1) * num_results
    #I calculated the start and end indices for the page.
    return df.iloc[start:end].to_dict(orient="records")
    #I returned the data for the page as a list of dictionaries.


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prep_dir, "prepared_data.csv")

    if not os.path.exists(file_path):
        return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

    df = pd.read_csv(file_path, dtype={
        'icao': str
    })

    if "icao" not in df.columns or df.empty:
        return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}
    #If the icao column is not in the dataframe or the dataframe is empty, I return the default values.

    aircraft_data = df[df["icao"] == icao].copy()
    #I filtered the dataframe for the given icao.

    if aircraft_data.empty:
        return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}
    #If the filtered dataframe is empty, I return the default values.

    aircraft_data["altitude_baro"] = aircraft_data["altitude_baro"].replace("ground", 0)
    aircraft_data["ground_speed"] = aircraft_data["ground_speed"].replace("ground", 0)

    aircraft_data["altitude_baro"] = pd.to_numeric(aircraft_data["altitude_baro"], errors="coerce")
    aircraft_data["ground_speed"] = pd.to_numeric(aircraft_data["ground_speed"], errors="coerce")


    aircraft_data["had_emergency"] = aircraft_data["had_emergency"].astype(bool)
    stats = aircraft_data.agg({
        "altitude_baro": "max",
        "ground_speed": "max",
        "had_emergency": "any"
    }).to_dict()
    #I calculated the max altitude baro, max ground speed, and if the aircraft had an emergency.

    stats["max_altitude_baro"] = stats.pop("altitude_baro", 0)
    stats["max_ground_speed"] = stats.pop("ground_speed", 0)
    #I renamed the keys in the stats dictionary.

    return stats
    #I returned the stats dictionary.
