echo "The first report - by category"
hive -f daily_reports_hql_hive_cli/by_category.hql --outputformat=csv2 > by_category_report.csv
echo "The second report - top 10 goods"
hive -f daily_reports_hql_hive_cli/top_10_goods.hql --outputformat=csv2 > top_10_goods.csv
echo "The third report - top 10 goods sale"
hive -f daily_reports_hql_hive_cli/top_10_goods_sale.hql --outputformat=csv2 > top_10_goods_sale.csv
echo "The last report - top 10 difference"
hive -f daily_reports_hql_hive_cli/top_10_difference.hql --outputformat=csv2 > top_10_difference.csv