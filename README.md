# BDT Semestral Project by Anton Kretov - Sales analysis

The project had three logical stages:

1. Create the whole infrastructure:
	- First of all, I created external and managed tables in Hive so that the sales data can be uploaded to Hadoop. There were also several helper tables for items and categories. For creating these tables and filling helper tables with data I firstly uploaded these .CSV files into corresponding HDFS directories, then I used them as a reference to the external table for items and categories. As for daily sales logs, I created script *upload_script.sh*, which is called by CRON job every day at 7:30 and scans the predefined dumps directory and looks for .CSV files for the new logs. Then, it puts this file to HDFS cluster and runs Hive INSERT quiery. As a result, the managed table sales_log is been updated with new data. For the purposes of this project I split the sales dataset into chucks, each of them denotes one particular day. Then, I experimented on 2 years data. The results and the notebook presented here are computed for this particular timespan.

2. Get daily reports:
	Daily reports consist of 4 pretty easy to understand quieries, which are stored in different .hql files in *daily_reports_hql* directory (for running with the help of beeline cli) or in *daily_reports_hql_hive_cli* for running in hive cli. These quieries are automatically called by invoking *daily_report_run.sh* script (for beeline cli) and *daily_report_run_hive_cli.sh* script (for hive cli). The outputs are then saved into .csv files.

3. Exploratory analysis:
	Exploratory analysis has been conducted locally in *Spark Analysis Local.ipynb* file, the same results are produced by running *spark_analysis_cluster.py* file on Hador (where there is Spark Context connected to HDFS). The result of running this script in the following way:

	* export PYSPARK_PYTHON=python3
	* pyspark --master yarn --num-executors 2 --executor-memory 4G --conf spark.ui.port=10910 < spark_analysis_cluster.py

	consists of different .csv outputs and plots. The explanation for all the results is provided in the .ipynb file.