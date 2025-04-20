from sklearn.model_selection import train_test_split
import pandas as pd
from datetime import datetime
from helper.utils import etl_log  # Make sure this utility function is available

def split_data(df: pd.DataFrame, features: list, target: str, test_size=0.2, random_state=42):
    """
    Split dataset into train and test sets
    """
    try:
        X = df[features]
        y = df[target]
        
        # Log success message before splitting
        log_msg = {
            "step": "modelling",
            "component": "split_data",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)
        
        return train_test_split(X, y, test_size=test_size, random_state=random_state)

    except Exception as e:
        log_msg = {
            "step": "modelling",
            "component": "split_data",
            "status": "failed",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        etl_log(log_msg)
        raise
