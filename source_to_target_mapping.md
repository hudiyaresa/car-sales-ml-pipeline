| Source         | Source Field / Logic        | Transformation                  | Target Table | Target Column   |
|----------------|------------------------------|----------------------------------|--------------|------------------|
| DB: car_sales  | id_sales                     | Direct copy                      | car_sales    | id_sales         |
| DB: car_sales  | year                         | Convert int → varchar            | car_sales    | year             |
| DB: car_sales  | brand_car                    | Direct                           | car_sales    | brand_car        |
| DB: car_sales  | transmission                 | Direct                           | car_sales    | transmission     |
| DB: car_sales  | state                        | Direct                           | car_sales    | state            |
| DB: car_sales  | condition                    | Convert float4 → varchar         | car_sales    | condition        |
| DB: car_sales  | odometer                     | Convert float4 → varchar         | car_sales    | odometer         |
| DB: car_sales  | color                        | Direct                           | car_sales    | color            |
| DB: car_sales  | interior                     | Direct                           | car_sales    | interior         |
| DB: car_sales  | mmr                          | Convert float4 → varchar         | car_sales    | mmr              |
| DB: car_sales  | sellingprice                 | Convert float4 → varchar         | car_sales    | sellingprice     |
| System         | now()                        | Default timestamp                | car_sales    | created_at       |
| Spreadsheet    | brand_car_id                 | Direct                           | car_brand    | brand_car_id     |
| Spreadsheet    | brand_name                   | Direct                           | car_brand    | brand_name       |
| System         | now()                        | Default                          | car_brand    | created_at       |
| API            | id_state                     | Direct                           | us_state     | id_state         |
| API            | code                         | Direct                           | us_state     | code             |
| API            | name                         | Direct                           | us_state     | name             |
| System         | now()                        | Default                          | us_state     | created_at       |
