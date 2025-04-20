import pandas as pd
import joblib
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from datetime import datetime
from helper.utils import etl_log
from modelling.extract.extract_warehouse import extract_warehouse
from modelling.preprocessing.preprocessing_data import process_preprocessing
from modelling.preprocessing.splitting_data import split_data
import os

def linear_regression():
    # Load environment variables
    load_dotenv()

    # Extract data from warehouse database
    df = extract_warehouse(table_name="car_sales")

    # Define features and target
    features = ['year', 'condition', 'odometer', 'mmr']
    target = 'selling_price'

    try:
        # Step 1: Preprocess
        df_processed = process_preprocessing(df, features, target)

        # Log success message after preprocessing
        log_msg = {
            "step": "modelling",
            "component": "preprocess_data",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

        # Step 2: Split data
        X_train, X_test, y_train, y_test = split_data(df_processed, features, target)

        # Log success message after splitting data
        log_msg = {
            "step": "modelling",
            "component": "split_data",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

        # Step 3: Train model
        model = LinearRegression()
        model.fit(X_train, y_train)

        # Log success message after training model
        log_msg = {
            "step": "modelling",
            "component": "train_model",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

        # Step 4: Evaluate
        y_pred = model.predict(X_test)
        print("MAE:", mean_absolute_error(y_test, y_pred))
        print("MSE:", mean_squared_error(y_test, y_pred))
        print("R²:", r2_score(y_test, y_pred))

        # Log success message after evaluation
        log_msg = {
            "step": "modelling",
            "component": "evaluate_model",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

        # Step 5: Save model and dump to minIo
        model_filename = "car_price_model.pkl"
        joblib.dump(model, model_filename)

        # Upload the model to MinIO
        client = Minio('localhost:9000',
                    access_key=os.getenv('MINIO_ACCESS_KEY'),
                    secret_key=os.getenv('MINIO_SECRET_KEY'),
                    secure=False)
        
        # Make a bucket if it doesn't exist
        bucket_name = "car_sales_modelling"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        # Save the model to the MinIO bucket 'models'
        client.fput_object(bucket_name, model_filename, model_filename)

        # Log success message after saving model
        log_msg = {
            "step": "modelling",
            "component": "save_model",
            "status": "success",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        etl_log(log_msg)

    except Exception as e:
        log_msg = {
            "step": "modelling",
            "component": "linear_regression",
            "status": "failed",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        etl_log(log_msg)
        raise
