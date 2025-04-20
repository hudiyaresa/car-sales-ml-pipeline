import pandas as pd
import numpy as np

def clean_and_merge_categories(df):
    # Mapping for merging similar categories in the 'color' column
    color_mapping = {
        '—': '', '': '', '16633': '', '6388': '', 
        'off-white': 'white', 'white': 'white', 'gray': 'grey'
    }

    # Mapping for merging similar categories in the 'interior' column
    interior_mapping = {
        '—': '', '': '', 'off-white': 'white', 'white': 'white', 
        'gray': 'grey', 'green': 'green'
    }

    # Apply the category merging mappings to the 'color' and 'interior' columns
    df['color'] = df['color'].astype(str).str.lower().map(color_mapping).fillna(df['color'])
    df['interior'] = df['interior'].astype(str).str.lower().map(interior_mapping).fillna(df['interior'])
    
    return df

def drop_invalid_values(df):
    # Replace '' and '—' to np.nan and drop
    df = df.replace({'': np.nan, '—': np.nan})

    # Drop all row with NaN
    df = df.dropna()

    # Drop rows where 'condition' is NaN
    df = df[df['condition'].notna()]

    return df

def drop_sales_by_id(df):
    # Function to drop rows where 'id_sales' is 8013 or 26976
    df = df[~df['id_sales'].isin([8013, 26976])]

    # Drop rows with invalid state code
    invalid_states = ['3vwd17aj5fm219943', '3vwd17aj5fm297123']
    df = df[~df['state'].isin(invalid_states)]

    return df

def mapping_target(df, df_car_brand, df_us_state):
    # Map 'brand_car' from car_sales to 'brand_car_id' from car_brand table
    brand_mapping = dict(zip(df_car_brand['brand_name'], df_car_brand['brand_car_id']))
    df['brand_car_id'] = df['brand_car'].map(brand_mapping)

    # Map 'state' from car_sales to 'id_state' from us_state table
    state_mapping = dict(zip(df_us_state['code'], df_us_state['id_state']))
    df['id_state'] = df['state'].map(state_mapping)

    # Convert 'year' from varchar to int4
    df['year'] = pd.to_numeric(df['year'], errors='coerce', downcast='integer')

    # Convert 'condition', 'odometer', 'mmr', and 'sellingprice' from varchar to float4
    df['condition'] = pd.to_numeric(df['condition'], errors='coerce', downcast='float')
    df['odometer'] = pd.to_numeric(df['odometer'], errors='coerce', downcast='float')
    df['mmr'] = pd.to_numeric(df['mmr'], errors='coerce', downcast='float')
    df['sellingprice'] = pd.to_numeric(df['sellingprice'], errors='coerce', downcast='float')

    # Rename columns to match the warehouse schema
    df = df.rename(columns={
        'id_sales': 'id_sales_nk',
        'sellingprice': 'selling_price'
    })

    # Ensure all required columns are present and ordered correctly
    warehouse_columns = [
        'id_sales_nk',
        'year',
        'brand_car_id',
        'transmission',
        'id_state',
        'condition',
        'odometer',
        'color',
        'interior',
        'mmr',
        'selling_price',
        'created_at'
    ]

    # Filter columns to match the warehouse schema
    df = df[warehouse_columns]
    
    return df

def transform_car_sales(df, df_car_brand, df_us_state):
    # Step 1: Clean and merge categories
    df = clean_and_merge_categories(df)
    
    # Step 2: Drop rows with invalid values
    df = drop_invalid_values(df)
    
    # Step 3: Drop rows with 'id_sales' 8013 and 26976
    df = drop_sales_by_id(df)

    # Step 4: Mapping and transformation
    df = mapping_target(df, df_car_brand, df_us_state)
    
    return df
