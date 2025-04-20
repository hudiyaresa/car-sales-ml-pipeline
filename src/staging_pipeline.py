from staging.extract.extract_db import extract_database
from staging.extract.extract_api import extract_api
from staging.extract.extract_spreadsheet import extract_sheet
from staging.load.load_staging import load_staging
from staging.transform.transform_car_sales import transform_car_sales
from datetime import datetime
import os

def staging_pipeline():
    # Extract data from database
    df_car_sales = extract_database(table_name="car_sales")

    # Extract data from api
    df_us_state = extract_api(link_api="https://raw.githubusercontent.com/Kurikulum-Sekolah-Pacmann/us_states_data/refs/heads/main/us_states.json", list_parameter="", data_name="regions")

    # Extract data from spreadsheet
    df_car_brand = extract_sheet(key_file=os.getenv('KEY_SPREADSHEET'), worksheet_name="brand_car")

    # Transform car sales data from database
    tf_df_car_sales = transform_car_sales(df_car_sales)
    
    # Load data into staging (except last column, created_at)
    load_staging(data=tf_df_car_sales, schema='public', table_name='car_sales', idx_name='id_sales')
    load_staging(data=df_us_state, schema='public', table_name='us_state', idx_name='id_state')
    load_staging(data=df_car_brand, schema='public', table_name='car_brand', idx_name='brand_car_id')