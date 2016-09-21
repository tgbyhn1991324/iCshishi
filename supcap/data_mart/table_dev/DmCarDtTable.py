# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightCarDtTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import DeduplicationDate

def vehicleTableGet(sc,sqlContext):
	try:
		car_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_VEHICLE_INFO_TOTAL).map(lambda x: (x.vin,x[1:]))
	except:
		car_temp_rdd = sc.parallelize([])
	car_rows_a = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_A).map(lambda x: (x.vin,x[1:-2]))
	car_rows_b = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_B).map(lambda x: (x.vin,(x.rid,x.mileage_latest,x.start_date_latest,x.if_membership,x.if_used_car,x.if_onstar))) 
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	car_rows = car_rows_a.leftOuterJoin(car_rows_b).map(lambda x: (x[0],(x[1][0]+(x[1][1] or (None,)*6)+(today_str,today_str))))\
							.union(car_temp_rdd).reduceByKey(lambda x,y:DeduplicationDate(x,y)) \
							.map(lambda x: ((x[0],)+x[1])).coalesce(100)
	car_rows_schemardd = sqlContext.applySchema(car_rows, dt_vehicle_info_total_schema)
	car_schemardd = sqlContext.parquetFile(HDFS_PATH_DT_VEHICLE_INFO_TOTAL)
	car_schemardd.registerTempTable("dm_vehicle_info_total")
	car_rows_schemardd.insertInto("dm_vehicle_info_total",overwrite=True)	

if __name__ == '__main__':
	vehicleTableGet(sc,sqlContext)
