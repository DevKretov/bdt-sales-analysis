export HADOOP_CLIENT_OPTS="-Ddisable.quoting.for.sv=false"

echo "The first report - by category"

beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/kretov;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
	-f daily_reports_hql/by_category.hql \
	--silent=true \
	--verbose=false \
	--outputformat=csv2 > daily_reports_output/by_category_report.csv


echo "The second report - top 10 goods"

beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/kretov;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
	-f daily_reports_hql/top_10_goods.hql \
	--silent=true \
	--verbose=false \
	--outputformat=csv2 > daily_reports_output/top_10_goods.csv


echo "The third report - top 10 goods sale"

beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/kretov;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
	-f daily_reports_hql/top_10_goods_sale.hql \
	--silent=true \
	--verbose=false \
	--outputformat=csv2 > daily_reports_output/top_10_goods_sale.csv


echo "The last report - top 10 difference"

beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/kretov;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
	-f daily_reports_hql/top_10_difference.hql \
	--silent=true \
	--verbose=false \
	--outputformat=csv2 > daily_reports_output/top_10_difference.csv
