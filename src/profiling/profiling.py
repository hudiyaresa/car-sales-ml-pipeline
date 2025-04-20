import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from helper.utils import etl_log
from staging.extract.extract_api import extract_api
from staging.extract.extract_spreadsheet import extract_sheet
from staging.extract.extract_db import extract_database

def extract_all_sources():
    """Extract data from all sources and return combined dictionary."""
    combined_data = {}

    # Extract from database
    df_car_sales = extract_database(table_name="car_sales")
    combined_data["db_car_sales"] = df_car_sales

    # Extract from API
    df_api = extract_api(
        link_api="https://raw.githubusercontent.com/Kurikulum-Sekolah-Pacmann/us_states_data/refs/heads/main/us_states.json",
        list_parameter={},
        data_name="regions"
    )
    combined_data["api_us_state"] = df_api

    # Extract from Spreadsheet
    spreadsheet_key = os.getenv("KEY_SPREADSHEET")
    worksheet_name = "brand_car"
    df_sheet = extract_sheet(spreadsheet_key, worksheet_name)
    combined_data["sheet_brand_car"] = df_sheet

    return combined_data

def table_shapes(data):
    return {table: df.shape for table, df in data.items()}

def column_types(data):
    return {table: {col: str(df[col].dtype) for col in df.columns} for table, df in data.items()}

def unique_values(data):
    target_columns = ['state', 'body', 'color', 'interior']
    result = {}

    # Only check unique values for the database data (db_car_sales)
    if "db_car_sales" in data:
        df = data["db_car_sales"]
        result["db_car_sales"] = {}
        for col in target_columns:
            result["db_car_sales"][col] = df[col].unique().tolist() if col in df.columns else []
    return result

def missing_value_percent(data):
    result = {}
    for table, df in data.items():
        result[table] = {}
        for col in df.columns:
            # Include '' and '—' as missing value
            missing_mask = df[col].isnull() | (df[col] == '') | (df[col] == '—')
            missing_percentage = round(float(missing_mask.mean() * 100), 2)
            result[table][col] = missing_percentage
    return result


def profile_report():
    data = extract_all_sources()

    report = {
        "person_in_charge": "Reza",
        "date_profiling": str(datetime.now()),
        "result": {}
    }

    shape_result = table_shapes(data)
    type_result = column_types(data)
    unique_result = unique_values(data)
    missing_result = missing_value_percent(data)

    for table in data.keys():
        report["result"][table] = {
            "shape": shape_result[table],
            "data_types": type_result[table],
            "unique_values": unique_result.get(table, {}),
            "missing_percentage": missing_result[table]
        }

    # Save JSON
    os.makedirs("profiling/output_profiling", exist_ok=True)
    output_path = os.path.join("profiling/output_profiling", "car_sales_profiling_report.json")
    with open(output_path, "w") as f:
        json.dump(report, f, indent=4)

    return report

# Run it
profile_report()
