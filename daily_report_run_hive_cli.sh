echo "The first report - by category"
hive -S -f daily_reports_hql_hive_cli/by_category.hql |  python -c 'exec("import sys;import csv;reader = csv.reader(sys.stdin, dialect=csv.excel_tab);writer = csv.writer(sys.stdout, dialect=csv.excel)\nfor row in reader: writer.writerow(row)")' > daily_reports_output/by_category_report.csv
echo "The second report - top 10 goods"
hive -S -f daily_reports_hql_hive_cli/top_10_goods.hql |  python -c 'exec("import sys;import csv;reader = csv.reader(sys.stdin, dialect=csv.excel_tab);writer = csv.writer(sys.stdout, dialect=csv.excel)\nfor row in reader: writer.writerow(row)")' > daily_reports_output/top_10_goods.csv
echo "The third report - top 10 goods sale"
hive -S -f daily_reports_hql_hive_cli/top_10_goods_sale.hql |  python -c 'exec("import sys;import csv;reader = csv.reader(sys.stdin, dialect=csv.excel_tab);writer = csv.writer(sys.stdout, dialect=csv.excel)\nfor row in reader: writer.writerow(row)")' > daily_reports_output/top_10_goods_sale.csv
echo "The last report - top 10 difference"
hive -S -f daily_reports_hql_hive_cli/top_10_difference.hql |  python -c 'exec("import sys;import csv;reader = csv.reader(sys.stdin, dialect=csv.excel_tab);writer = csv.writer(sys.stdout, dialect=csv.excel)\nfor row in reader: writer.writerow(row)")' > daily_reports_output/top_10_difference.csv