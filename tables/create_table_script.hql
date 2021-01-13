-- Categories list --

CREATE EXTERNAL TABLE item_categories_ext

(
item_category_name STRING COMMENT "The name of the category",
item_category_id INTEGER COMMENT "The identifier of the category"
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION "/user/kretov/sales_data/categories"
tblproperties ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS item_categories
(
item_category_name STRING,
item_category_id INTEGER
)
STORED as PARQUET
tblproperties("parquet.compress"="SNAPPY");


INSERT OVERWRITE TABLE item_categories
SELECT i.item_category_name, i.item_category_id
from item_categories_ext i;


-- Items list --

CREATE EXTERNAL TABLE items_ext

(
item_name STRING COMMENT "The name of the product",
item_id INTEGER COMMENT "The identifier of the product",
item_category_id INTEGER COMMENT "The identifier of the category"
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION "/user/kretov/sales_data/items"

tblproperties ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS items
(
item_name STRING,
item_id INTEGER,
item_category_id INTEGER
)
stored as parquet
tblproperties("parquet.compress"="SNAPPY");


INSERT OVERWRITE TABLE items
SELECT i.item_name, i.item_id, i.item_category_id
from items_ext i;


-- Sales log --


CREATE EXTERNAL TABLE sales_daily_log_ext

(

sale_date STRING COMMENT "The date of the sale taken place in format dd/mm/yyyy",
month_num SMALLINT COMMENT "A consecutive month number, used for convenience",
shop_id SMALLINT COMMENT "A unique identifier of the shop",
item_id INTEGER COMMENT "A unique identifier of the item",
item_price FLOAT COMMENT "The price of the item in the moment of the purchase",
item_cnt_day FLOAT COMMENT "The number of products sold"

)

ROW FORMAT delimited
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/kretov/dumps_hdfs"
tblproperties ("skip.header.line.count"="1");


CREATE TABLE sales_log

(
sale_date STRING COMMENT "The date of the sale taken place in format dd/mm/yyyy",
shop_id SMALLINT COMMENT "A unique identifier of the shop",
item_id INTEGER COMMENT "A unique identifier of the item",
item_price FLOAT COMMENT "The price of the item in the moment of the purchase",
item_cnt_day FLOAT COMMENT "The number of products sold"
)

PARTITIONED BY (month_num SMALLINT)
STORED AS PARQUET
tblproperties("parquet.compress"="SNAPPY");




