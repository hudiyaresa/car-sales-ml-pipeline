import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import sqlalchemy
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Setup Logging Configuration

LOG_DIR = "log"
LOG_FILE = os.path.join(LOG_DIR, "info_process.log")

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging only once
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Database Connections
def get_db_connection(db_type):
    if db_type == 'source':
        return create_engine(f"postgresql://{os.getenv('SRC_POSTGRES_USER')}:{os.getenv('SRC_POSTGRES_PASSWORD')}@{os.getenv('SRC_POSTGRES_HOST')}:{os.getenv('SRC_POSTGRES_PORT')}/{os.getenv('SRC_POSTGRES_DB')}")
    elif db_type == 'staging':
        return create_engine(f"postgresql://{os.getenv('STG_POSTGRES_USER')}:{os.getenv('STG_POSTGRES_PASSWORD')}@{os.getenv('STG_POSTGRES_HOST')}:{os.getenv('STG_POSTGRES_PORT')}/{os.getenv('STG_POSTGRES_DB')}")
    elif db_type == 'warehouse':
        return create_engine(f"postgresql://{os.getenv('WH_POSTGRES_USER')}:{os.getenv('WH_POSTGRES_PASSWORD')}@{os.getenv('WH_POSTGRES_HOST')}:{os.getenv('WH_POSTGRES_PORT')}/{os.getenv('WH_POSTGRES_DB')}")
    elif db_type == 'log':
        return create_engine(f"postgresql://{os.getenv('LOG_POSTGRES_USER')}:{os.getenv('LOG_POSTGRES_PASSWORD')}@{os.getenv('LOG_POSTGRES_HOST')}:{os.getenv('LOG_POSTGRES_PORT')}/{os.getenv('LOG_POSTGRES_DB')}")

# Logging
def etl_log(log_msg: dict):
    # Write log to database
    try:
        conn = get_db_connection('log')
        df_log = pd.DataFrame([log_msg])
        df_log.to_sql(name="etl_log", con=conn, if_exists="append", index=False)
    except Exception as e:
        logging.error(f"Can't save log to DB. Cause: {str(e)}")

    # Write log to file
    try:
        log_line = ""

        for key, value in log_msg.items():
            log_line += f"{key}={value} | "

        log_line = log_line.rstrip(" | ")

        logging.info(log_line)
    except Exception as e:
        print(f"Can't save log to file. Cause: {str(e)}")

def read_etl_log(filter_params: dict) -> pd.DataFrame:
    """
    Reads the latest etl_date from the log table for incremental extraction.
    """
    try:
        # create connection to database        
        conn = get_db_connection('log')

        query = sqlalchemy.text("""
            SELECT MAX(etl_date) as latest_etl_date
            FROM etl_log
            WHERE 
                step = :step AND
                component = :component AND
                status = :status AND
                table_name ILIKE :table_name
        """)

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=filter_params)

        #return extracted data
        return df
    
    except Exception as e:
        print(f"Can't execute your query. Cause: {str(e)}")
        return pd.DataFrame()

# Read SQL Query from Table
def read_sql(table_name: str) -> str:
    """
    Generates a basic SQL query to select all rows from the specified table
    """
    # // where created_at is greater than a given etl_date (parameterized) (WHERE created_at > :etl_date). 
    query = f"SELECT * FROM {table_name}"
    return query

def read_sql_inc(table_name: str) -> str:
    """
    Generates a basic SQL query to select all rows from the specified table
    Where created_at is greater than a given etl_date (parameterized) (WHERE created_at > :etl_date). 
    """
    query = f"SELECT * FROM {table_name} WHERE created_at > :etl_date"
    return query

# Create Function handle_error to dump failure data to MiniO
def handle_error(data, bucket_name: str, table_name: str, step: str, component: str):
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Initialize MinIO client
    client = Minio('localhost:9000',
                access_key=os.getenv('MINIO_ACCESS_KEY'),
                secret_key=os.getenv('MINIO_SECRET_KEY'),
                secure=False)
    
    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to CSV and then to bytes
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # Upload the CSV file to the bucket
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{step}_{component}_{table_name}_{current_date}.csv",
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    # List objects in the bucket
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print(obj.object_name)