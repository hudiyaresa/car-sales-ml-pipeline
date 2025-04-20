import pandas as pd

def transform_datatype_car_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms car_sales data according to the source-to-target mapping.
    """

    # Define the columns to keep
    selected_columns = [
        "id_sales", "year", "brand_car", "transmission", "state",
        "condition", "odometer", "color", "interior", "mmr", "sellingprice"
    ]

    # Select only the necessary columns
    df = df[selected_columns].copy()

    # Apply transformations
    df["year"] = df["year"].astype(str)
    df["condition"] = df["condition"].astype(str)
    df["odometer"] = df["odometer"].astype(str)
    df["mmr"] = df["mmr"].astype(str)
    df["sellingprice"] = df["sellingprice"].astype(str)

    return df
