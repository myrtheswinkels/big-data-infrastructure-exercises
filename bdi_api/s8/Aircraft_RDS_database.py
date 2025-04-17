import gzip
import json
from io import BytesIO

import boto3
import psycopg2
from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()

def connection_postgres():
    conn = psycopg2.connect(
        host=db_credentials.host,
        dbname=db_credentials.database,
        user=db_credentials.username,
        password=db_credentials.password,
        port=db_credentials.port
    )
    return conn
# AWS S3 and RDS configuration
s3_bucket_name = settings.s3_bucket  # Replace with your S3 bucket name
# s3_bucket_name = "bdi-aircraft-myrthe"
s3_file_key = "aircraft_database/AircraftDatabase.zip"  

table_name = "aircraft_database"  

#Download the Gzip file from S3
s3 = boto3.client("s3")
try:
    print("Downloading Gzip file from S3...")
    s3_object = s3.get_object(Bucket=s3_bucket_name, Key=s3_file_key)
    gzip_content = BytesIO(s3_object["Body"].read())
    print("Gzip file downloaded successfully.")
except Exception as e:
    print(f"Error downloading Gzip file from S3: {e}")
    exit()

#Decompress the Gzip file
try:
    print("Decompressing Gzip file...")
    with gzip.GzipFile(fileobj=gzip_content) as gz:
        json_content = gz.read().decode("utf-8")
    print("Gzip file decompressed successfully.")
except Exception as e:
    print(f"Error decompressing Gzip file: {e}")
    exit()

#Parse the JSON content
try:
    print("Parsing NDJSON content...")
    data = []
    for line in json_content.splitlines(): 
        if line.strip():  
            data.append(json.loads(line))  
    print(f"Parsed {len(data)} JSON objects successfully.")
except Exception as e:
    print(f"Error parsing NDJSON content: {e}")
    exit()

#Insert the data into the RDS database
try:
    print("Connecting to the RDS database...")
    conn = connection_postgres()
    
    cursor = conn.cursor()
    print("Connected to the database.")

    # Insert each record into the database
    for record in data:
        # Replace empty strings with None
        sanitized_record = {key: (value if value != "" else None) for key, value in record.items()}
        keys = sanitized_record.keys()
        values = tuple(sanitized_record.values())
        cursor.execute(
            f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))})",
            values
        )

    # Commit the transaction
    conn.commit()
    print("Data inserted into RDS successfully.")
except Exception as e:
    print(f"Error inserting data into RDS: {e}")
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")
