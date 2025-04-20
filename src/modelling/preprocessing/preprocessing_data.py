import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
from datetime import datetime
from helper.utils import etl_log  # Make sure this utility function is available

def process_preprocessing(df: pd.DataFrame, features: list, target: str) -> pd.DataFrame:
    """
    Perform preprocessing: handle nulls, encoding, and scaling.

    Steps:
    1. Drop rows with '' or 'unknown' in features or target
    2. Encode categorical columns
    3. Scale numerical columns
    """

    try:
        # Step 1: Drop rows containing '' or 'unknown'
        # df = df[~df[features + [target]].isin(['', 'unknown']).any(axis=1)]
        
        # Log success message after cleaning missing data
        # log_msg = {
        #     "step": "modelling",
        #     "component": "preprocessing_drop_rows",
        #     "status": "success",
        #     "table_name": "car_sales",
        #     "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # }
        # etl_log(log_msg)

        # Step 1: Encode categorical columns
        categorical_cols = df[features].select_dtypes(include='object').columns.tolist()
        for col in categorical_cols:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
        
        # Log success message after encoding categorical features
        log_msg = {
            "step": "modelling",
            "component": "preprocessing_encode_categorical",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

        # Step 2: Scale numerical columns
        numerical_cols = df[features].select_dtypes(include=['int64', 'float64']).columns.tolist()
        scaler = StandardScaler()
        df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

        # Log success message after scaling numerical features
        log_msg = {
            "step": "modelling",
            "component": "preprocessing_scale_numerical",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)
        
    except Exception as e:
        log_msg = {
            "step": "modelling",
            "component": "preprocessing_process_preprocessing",
            "status": "failed",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        etl_log(log_msg)
        raise

    return df
