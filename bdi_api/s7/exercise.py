import gzip
import io
import json

import boto3
import duckdb
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, status

from bdi_api.settings import DBCredentials, Settings

load_dotenv()

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

# AWS S3 Configuration
S3_BUCKET = settings.s3_bucket
S3_PREFIX = "data/20231101/"
DB_FILE = "aircraft_data.db"

# Initialize S3 client
s3 = boto3.client("s3")

def connection_duckdb():
    """Establishes a connection to DuckDB"""
    return duckdb.connect(DB_FILE)

def connection_postgres():
    conn = psycopg2.connect(
        host=db_credentials.host,
        dbname=db_credentials.database,
        user=db_credentials.username,
        password=db_credentials.password,
        port=db_credentials.port
    )
    return conn

def get_file_from_s3(file_key):
    """Fetch JSON data from S3 and handle Gzip files correctly."""
    # Prevent directory processing
    if file_key.endswith("/"):
        return []

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)

        # Check if the content type is a directory
        if obj.get("ContentType") == "application/x-directory":
            return []

        # Check if the file is empty
        if obj["ContentLength"] == 0:
            return []

        content = obj["Body"].read()

        if not content:
            return []

        # Handle Gzip files
        if content[:2] == b'\x1f\x8b':
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                content = gz.read()

        # Decode and parse JSON
        decoded_content = content.decode("utf-8", errors="ignore").strip()
        if not decoded_content:
            return []

        data = json.loads(decoded_content)

        return data.get("aircraft", []) if isinstance(data, dict) else data

    except s3.exceptions.NoSuchKey:
        print(f"File {file_key} does not exist in S3.")
        return []
    except json.JSONDecodeError:
        print(f"Invalid JSON in file {file_key}")
        return []
    except Exception as e:
        print(f"Error fetching file {file_key}: {e}")
        return []

def get_file_from_s3_timestamp(file_key):
    """Fetch JSON data from S3 and handle Gzip files correctly."""
    # Prevent directory processing
    if file_key.endswith("/"):
        return []

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)

        # Check if the content type is a directory
        if obj.get("ContentType") == "application/x-directory":
            return []

        # Check if the file is empty
        if obj["ContentLength"] == 0:
            return []

        content = obj["Body"].read()

        if not content:
            print(f"File {file_key} has no content.")
            return []

        # Handle Gzip files
        if content[:2] == b'\x1f\x8b':  # Gzip magic number
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                content = gz.read()

        # Decode and parse JSON
        decoded_content = content.decode("utf-8", errors="ignore").strip()
        if not decoded_content:
            return []

        data = json.loads(decoded_content)

        return data.get("now", []) if isinstance(data, dict) else data

    except s3.exceptions.NoSuchKey:
        print(f"File {file_key} does not exist in S3.")
        return []
    except json.JSONDecodeError:
        print(f"Invalid JSON in file {file_key}")
        return []
    except Exception as e:
        print(f"Error fetching file {file_key}: {e}")
        return []


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from S3 and insert it into RDS"""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    if "Contents" not in response:
        return "No files found in S3 bucket."

    all_data = []
    columns_to_keep = ['hex', 'type', 'r', 't', 'alt_baro', 'gs', 'emergency', 'lat', 'lon', 'timestamp']
    emergency_types = {'general', 'lifeguard', 'minfuel', 'reserved', 'nordo', 'unlawful', 'downed'}

    for obj in response["Contents"]:
        file_key = obj["Key"]

        data = get_file_from_s3(file_key)
        timestamp = get_file_from_s3_timestamp(file_key)

        if not isinstance(data, list) or len(data) == 0:
            print(f"Skipping file {file_key}: No valid aircraft data found.")
            continue  # Skip invalid files

        df = pd.DataFrame(data)
        df["timestamp"] = timestamp

        df = df[[col for col in columns_to_keep if col in df.columns]]

        # Rename columns to match database structure from the start
        df.rename(
            columns={
                "hex": "icao24",
                "r": "registration",
                "t": "type2",
                "type": "type",
                "alt_baro": "altitude",
                "gs": "velocity",
                "lat": "latitude",
                "lon": "longitude"
            },
            inplace=True
        )

        df = df.astype(str)

        df = df[~df["icao24"].str.startswith("~")]
        if "emergency" in df.columns:
            df["had_emergency"] = df["emergency"].apply(lambda x: x in emergency_types)
            df.drop(columns=["emergency"], inplace=True)
        else:
            df["had_emergency"] = False

        df.dropna(subset=["icao24", "registration", "type", "latitude", "longitude"], inplace=True)
        df.drop_duplicates(subset=["icao24", "timestamp"], inplace=True)

        all_data.append(df)

    if not all_data:
        return "No valid aircraft data found in any files."

    # Concatenate all data frames
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    # If final_df is empty, return early
    if final_df.empty:
        return "No valid aircraft data to insert."

    # Convert data types for numeric columns
    numeric_columns = ["altitude", "velocity", "latitude", "longitude", "timestamp"]
    for col in numeric_columns:
        if col in final_df.columns:
            # Convert to numeric
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')


    # Convert boolean column
    if "had_emergency" in final_df.columns:
        final_df["had_emergency"] = final_df["had_emergency"].astype(bool)

    else:
        final_df["had_emergency"] = False

    # Ensure all required columns exist
    required_columns = ["icao24", "type", "registration", "type2", "altitude", "velocity",
                       "latitude", "longitude", "timestamp", "had_emergency"]

    for col in required_columns:
        if col not in final_df.columns:
            # Add missing columns with appropriate default values
            if col in numeric_columns:
                final_df[col] = 0.0
            elif col == "had_emergency":
                final_df[col] = False
            else:
                final_df[col] = ""
            print(f"Added missing column: {col}")

    # Reorder columns to match SQL table
    final_df = final_df[required_columns]

    conn = None
    cursor = None
    rows_inserted = 0
    rows_failed = 0

    try:
        # Connect to the database
        conn = connection_postgres()
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            # delete table if exists
            cursor.execute("DROP TABLE IF EXISTS aircraft_data")
            conn.commit()
        except Exception:
            conn.rollback()

        # Create table if not exists
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aircraft_data (
                    icao24 VARCHAR,
                    type VARCHAR,
                    registration VARCHAR,
                    type2 VARCHAR,
                    altitude FLOAT,
                    velocity FLOAT,
                    latitude FLOAT,
                    longitude FLOAT,
                    timestamp FLOAT,
                    had_emergency BOOLEAN
                )
            """)
            conn.commit()
        except Exception:
            conn.rollback()

        # Test with a single row first
        if not final_df.empty:

            batch_size = 100
            total_rows = len(final_df)

            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                batch_df = final_df.iloc[batch_start:batch_end]
                batch_inserted = 0
                batch_failed = 0

                # Process each row in the batch
                for _, row in batch_df.iterrows():
                    try:
                        row_data = (
                            str(row['icao24']),
                            str(row['type']),
                            str(row['registration']),
                            str(row['type2']),
                            float(row['altitude']) if pd.notna(row['altitude']) and row['altitude'] is not None
                            else None,
                            float(row['velocity']) if pd.notna(row['velocity']) and row['velocity'] is not None
                            else None,
                            float(row['latitude']) if pd.notna(row['latitude']) and row['latitude'] is not None
                            else None,
                            float(row['longitude']) if pd.notna(row['longitude']) and row['longitude'] is not None
                            else None,
                            int(row['timestamp']) if pd.notna(row['timestamp']) and row['timestamp'] is not None
                            else None,
                            bool(row['had_emergency']) if pd.notna(row['had_emergency']) else None
                        )

                        cursor.execute("""
                            INSERT INTO aircraft_data
                            (icao24, type, registration, type2, altitude, velocity, latitude, longitude,
                                       timestamp, had_emergency)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, row_data)

                        batch_inserted += 1
                    except Exception as e:
                        batch_failed += 1
                        if batch_failed <= 3:  # Only show the first few errors
                            print(f"Row error: {e}")
                            print(f"Problematic row: {row.to_dict()}")

                # Commit the batch
                if batch_inserted > 0:
                    conn.commit()
                    rows_inserted += batch_inserted
                if batch_failed > 0:
                    conn.rollback()  # Roll back any failed inserts
                    rows_failed += batch_failed

    except Exception:
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    result_message = f"Data insertion complete. Inserted {rows_inserted} rows, failed {rows_failed} rows."
    return result_message


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = connection_postgres()
    cursor = conn.cursor()

    offset = page * num_results
    query = """
        SELECT icao24, registration, type2
        FROM aircraft_data
        ORDER BY icao24 ASC
        LIMIT %s OFFSET %s;
    """

    cursor.execute(query, (num_results, offset))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in rows]


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = connection_postgres()
    cursor = conn.cursor()

    offset = page * num_results
    query = """
        SELECT timestamp, latitude, longitude
        FROM aircraft_data
        WHERE icao24 = %s
        ORDER BY timestamp ASC
        LIMIT %s OFFSET %s;
    """

    cursor.execute(query, (icao, num_results, offset))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in rows]


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = connection_postgres()
    cursor = conn.cursor()

    query ="""
        SELECT
            MAX(altitude) AS max_altitude,
            MAX(velocity) AS max_velocity,
            BOOL_OR(had_emergency) AS had_emergency
        FROM aircraft_data
        WHERE icao24 = %s;
    """

    cursor.execute(query, (icao,))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return {"max_altitude_baro": row[0], "max_ground_speed": row[1], "had_emergency": bool(row[2])
            if row[2] is not None else False}

