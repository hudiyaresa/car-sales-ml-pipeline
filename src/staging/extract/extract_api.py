import pandas as pd
from dotenv import load_dotenv
import requests
from datetime import datetime
from helper.utils import etl_log

def extract_api(link_api:str, list_parameter:dict, data_name:str) -> pd.DataFrame:
    log_msg = {
        "step": "staging",
        "component": "extract_api",
        "table_name": data_name,
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        # Establish connection to API        
        resp = requests.get(link_api, params=list_parameter)
        resp.raise_for_status()  # Raises error if status is not 200

        # Parse the response JSON
        raw_response = resp.json()

        # Convert the JSON data to a pandas DataFrame        
        df_api = pd.DataFrame(raw_response)

        # Convert the key into a list and return it as a DataFrame
        df_result = pd.DataFrame(df_api[data_name].tolist())

        # create success log message
        log_msg["status"] = "success"
        return df_result

    except requests.exceptions.RequestException as e:
        # create fail log message        
        print(f"API request error: {e}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
        return pd.DataFrame()

    except ValueError as e:
        # create fail log message        
        print(f"JSON parsing error: {e}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
        return pd.DataFrame()

    finally:
        etl_log(log_msg)