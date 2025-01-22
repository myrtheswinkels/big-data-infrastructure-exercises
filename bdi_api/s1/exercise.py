import os
import requests
import json 
import csv
import pandas as pd
from typing import Annotated
from bs4 import BeautifulSoup

from fastapi import APIRouter, status
from fastapi.params import Query

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
    # TODO Implement download
    
    #Step 1: if file(s) exists, delete old file(s). 
    if os.path.exists(download_dir):
        for file in os.listdir(download_dir):
            file_path = os.path.join(download_dir, file)
            os.remove(file_path)
    
    os.makedirs(download_dir, exist_ok=True)
    #This first step is to get a list of all the files that are in the directory. This will delete the exiting files 
    #in the directory so that I can download the new ones.

    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    #I used BeutifulSoup to parse the html file and get the links to the files that I need to download.

    #Step 2: Download the new ones
    for i in soup.find_all('a', limit = file_limit):
        #I used the find_all method to get all the links that are in the html file. I used the limit parameter to limit the number of files that I download.
        href = i['href']
        #i created a variable href to store the href attribute of the link and if the href attribute ends with .json.gz, I will download the file.
        if href.endswith(".json.gz"):
            file_url = os.path.join(base_url, href)
            local_file_path = os.path.join(download_dir, href[:-3])
            #I used href[:-3] to remove the .gz extension from the file name. So now the file name will be a json file.

            file_response = requests.get(file_url)
            file_response.raise_for_status()

            #Write the file to the local directory and save it as a json file
            with open(local_file_path, "wb") as f:
                f.write(file_response.content)

    #Step 3: Return str
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
    # TODO
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")

    if os.path.exists(prep_dir):
        for file in os.listdir(prep_dir):
            file_path = os.path.join(prep_dir, file)
            os.remove(file_path)

    os.makedirs(prep_dir, exist_ok=True)
    #This first step is to get a list of all the files that are in the prep directory. This will delete the exiting files 
    #in the directory so that I can add the new ones.

    columns_to_keep = ['hex','type', 'r', 't', 'alt_baro', 'gs', 'emergency', 'lat', 'lon','timestamp']
    #These are the columns that I want to keep in the data, these are used in the following exercises. I will drop the columns that are not in this list.
    emergencies = ['general', 'lifeguard','minfuel','reserved','nordo','unlawful','downed']
    #These are the emergency types that I will use in the data, except 'none'. I will use this to check if the emergency column has any of these values.

    for file in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file)
        with open(file_path, "rb") as f:
            data = json.load(f)
            #I used the json.load method to load the json file and store it in the data variable.
            timestamp = data["now"] 
            #I figured out that the timestamp is in the now key of the json file. I stored this in the timestamp variable.
            if "aircraft" in data:
                #I checked if the aircraft key is in the data variable. If it is, I will create a DataFrame with the data and save it as a csv file.
                #I chose csv because it is easier to work with and I can easily read the data with pandas.
                df = pd.DataFrame(data["aircraft"])
                df["timestamp"] = timestamp
                df = df[[col for col in columns_to_keep if col in df.columns]]
                df = df.rename(columns={"hex":"icao", "r":"registration", "t":"type", "alt_baro":"altitude_baro", "gs":"ground_speed"})
                df = df[~df['icao'].str.startswith('~')]
                #I dropped the rows that have the icao column starting with ~, because these are not valid icao numbers.
                df['emergency'] = df['emergency'].apply(lambda x: x in emergencies)
                #I used the apply method to check if the emergency column has any of the values in the emergencies list. If it does, I will return True, else False.
                df = df.dropna(subset=['icao', 'registration', 'type', 'lat', 'lon'])
                #if any of the columns in the list is NaN, I will drop the row.
                df.to_csv(os.path.join(prep_dir, file[:-4]+"csv"), index=False)

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    
    if not os.path.exists(prep_dir):
        return "directory doesn't exist"
    
    #I created an empty DataFrame to store all the aircraft data from the csv files.
    all_data = pd.DataFrame()
    
    #I concatenate all csv files into one DataFrame, so i can sort and filter the data easily.
    for file in os.listdir(prep_dir):
        file_path = os.path.join(prep_dir, file)
        df = pd.read_csv(file_path)
        all_data = pd.concat([all_data, df], ignore_index=True)
       
    #I filtered the data on the columns that I need and dropped the duplicates. I sorted the data by icao number.
    all_data = all_data[["icao", "registration", "type"]].drop_duplicates()
    all_data = all_data.sort_values(by="icao", ascending=True)
    
    #I used the page and num_results parameters to get the data that I need. I used the iloc method to get the data and stored it in the result variable.
    start = page * num_results
    end = start + num_results
    result = all_data.iloc[start:end].to_dict(orient="records")
    
    return result 


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.

    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    #I created an empty DataFrame to store all the positions of the aircraft.
    all_positions = pd.DataFrame()

    if os.path.exists(prep_dir):
        for file in os.listdir(prep_dir):
            file_path = os.path.join(prep_dir, file)
            df = pd.read_csv(file_path)
            df = df[df['icao'] == icao]
            #if the given icao number is in the data, I will concatenate the data to the all_positions DataFrame.
            if not df.empty:
                all_positions = pd.concat([all_positions, df], ignore_index=True)

        #I added this if statement to return an empty list if the all_positions DataFrame is empty. So when a aircraft does not exist, it will return an empty list.        
        if all_positions.empty:
            return []
        
        #I filtered the data on the columns that I need and sorted the data by timestamp.
        all_positions = all_positions[['timestamp', 'lat', 'lon']].sort_values(by='timestamp')
        
        #I used the page and num_results parameters to get the data that I need. I used the iloc method to get the data and stored it in the result variable.      
        start = page * num_results
        end = start + num_results
        result = all_positions.iloc[start:end].to_dict(orient='records')

    return result


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    prep_dir = os.path.join(settings.prepared_dir, "day=20231101")
    
    #I created a dictionary to store the statistics of the aircraft.
    aircraft_stats = {
        "max_altitude_baro": 0,
        "max_ground_speed": 0,
        "had_emergency": False
    }
    
    if not os.path.exists(prep_dir):
        return aircraft_stats
        
    #Process every file in the prep directory
    for file in os.listdir(prep_dir):
        if not file.endswith(".csv"):
            continue
            
        file_path = os.path.join(prep_dir, file)
        
        #Read only the aircraft data with the given icao number.
        df = pd.read_csv(file_path)
        aircraft_data = df[df['icao'] == icao]
        
        if not aircraft_data.empty:
            #Find the maximum altitude baro for the given icao number
            max_alt = pd.to_numeric(aircraft_data['altitude_baro'], errors='coerce').max()
            if pd.notna(max_alt):
                aircraft_stats["max_altitude_baro"] = max(aircraft_stats["max_altitude_baro"], max_alt)
            
            #Find the maximum ground speed for the given icao number
            max_speed = pd.to_numeric(aircraft_data['ground_speed'], errors='coerce').max()
            if pd.notna(max_speed):
                aircraft_stats["max_ground_speed"] = max(aircraft_stats["max_ground_speed"], max_speed)
            
            #Find out if the aircraft had an emergency during the flight/timeperiod
            if any(aircraft_data['emergency'].fillna(False)):
                aircraft_stats["had_emergency"] = True

    return aircraft_stats                          
    
