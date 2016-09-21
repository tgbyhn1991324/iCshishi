# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDtPartTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)
	try:
		from pyspark.sql.types import *
	except:
		from pyspark.sql import *

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import DeduplicationDate

#将表DM_BO_REPAIR_PART_INFO根据key值（asc_code，balance_no，ro_no）union表DM_BO_SALE_PART_INFO，
#再根据key值（asc_code，balance_no，ro_no）join表DT_REPAIR_ORDER_TOTAL生成DT_BO_REPAIR_PART_TOTAL
def getRepairSalePartTable(sc,sqlContext):
	repair_balance = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL)
	repair_balance.registerTempTable("repair_balance")
	repair_part_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_REPAIR_PART_INFO).map(lambda x: ((x.asc_code,x.balance_no,x.ro_no),(x.part_quantity,x.part_sale_amount)))
	sale_part_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_SALE_PART_INFO).map(lambda x: ((x.asc_code,x.balance_no,x.ro_no),(x.part_quantity,x.part_sale_amount)))
	part_rdd = repair_part_rdd.union(sale_part_rdd).reduceByKey(lambda x,y: (str(float(x[0])+float(y[0])),str(float(x[1])+float(y[1]))))
	part_rdd_rows = part_rdd.map(lambda x: (x[0]+x[1]))
	part_schema = StructType([StructField("asc_code", StringType(), True),
							StructField("balance_no", StringType(), True),
							StructField("ro_no", StringType(), True),
							StructField("part_quantity", StringType(), True),
							StructField("part_sale_amount", StringType(), True)])
	part_schemardd = sqlContext.applySchema(part_rdd_rows,part_schema)
	part_schemardd.registerTempTable("repair_sale")

	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		dtRepairpartTotal_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_BO_REPAIR_PART_TOTAL).map(lambda x: (x[0:3],(x[3:])))
	except:
		dtRepairpartTotal_temp_rdd = sc.parallelize([])
	repair_balance = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL)
	repair_balance.registerTempTable("repair_balance")
	part_union_rdd = sqlContext.sql("select a.asc_code,a.balance_no,a.ro_no,b.vin,b.start_time,a.part_quantity,a.part_sale_amount from repair_sale a join repair_balance b on a.asc_code = b.asc_code and a.balance_no=b.balance_no and a.ro_no=b.ro_no")
	part_union_rows = part_union_rdd.map(lambda x: (x[0:3],(x[3:]+(today_str,today_str)))) \
						.union(dtRepairpartTotal_temp_rdd).repartition(1000) \
						.reduceByKey(DeduplicationDate) \
						.map(lambda x: (x[0]+x[1])) 
	part_union_rdd_schemardd = sqlContext.applySchema(part_union_rows,dt_bo_repair_part_total_schema)
	part_union = sqlContext.parquetFile(HDFS_PATH_DT_BO_REPAIR_PART_TOTAL)
	part_union.registerTempTable("dt_repair_part_total")
	part_union_rdd_schemardd.insertInto("dt_repair_part_total",overwrite=True)

if __name__ == '__main__':
	getRepairSalePartTable(sc,sqlContext)