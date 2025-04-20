from warehouse.extract.extract_stg import extract_staging
from warehouse.transform.transform_car_sales import transform_car_sales
from warehouse.load.load_wh import load_warehouse

def warehouse_pipeline():
    # Extract data from staging
    stg_car_sales = extract_staging("car_sales")
    stg_us_state = extract_staging("us_state")
    stg_car_brand = extract_staging("car_brand")

    # Transform car sales data from staging
    tf_stg_car_sales = transform_car_sales(df=stg_car_sales,df_car_brand=stg_car_brand,df_us_state=stg_us_state)

    # Load data into warehouse
    load_warehouse(data=tf_stg_car_sales, schema='public', table_name='car_sales', idx_name='id_sales_nk')