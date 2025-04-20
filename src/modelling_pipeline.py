
from modelling.extract.extract_warehouse import extract_warehouse
from modelling.preprocessing.preprocessing_data import process_preprocessing
from modelling.preprocessing.splitting_data import split_data

TARGET_COL = selling_price
TEST_SIZE = 0.2


df_wh = extract_warehouse(table_name = WH_TABLE_NAME)


# preprocessing data
df_wh = process_preprocessing(data = df_wh)

# splitting data
X_train, X_test, y_train, y_test = split_data(data = df_wh,
                                                        target_col = TARGET_COL,
                                                        test_size = TEST_SIZE)