#!/bin/sh

dump_file_path="/storage/brno2/home/kretov/semestral_project/dumps"
dump_file_path_old="/storage/brno2/home/kretov/semestral_project/dumps_old/"
username="kretov"

query="INSERT INTO TABLE sales_log \
PARTITION (month_num) \
SELECT sale_date, shop_id, item_id, item_price, item_cnt_day, month_num \
FROM sales_daily_log_ext;"

for d in /storage/brno2/home/kretov/semestral_project/dumps/*; do
	path="${d}"
	basename_=$(basename $path)
    echo "$path exists. Exporting it"
    hdfs dfs -put $path "dumps_hdfs/${basename_}"
    hive -e "set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; use $username; $query"

    rewrite_filename=$( date "+%D" | sed 's+/+_+g' )
    rewrite_filename="${rewrite_filename}.csv"

    hdfs dfs -mv "dumps_hdfs/${basename_}" "dumps_hdfs_old/${basename_}"
    mv $path "${dump_file_path_old}/${basename_}"
done