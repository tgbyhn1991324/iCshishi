# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDtClaimTableTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import DeduplicationDate

#表DM_CLAIM_ORDER_INFO_ALL根据key值(asc_code，ro_no)join表DT_REPAIR_ORDER_TOTAL
#生成DT_CLAIM_ORDER_INFO
def getClaimTable(sc,sqlContext):
	claim_order = sqlContext.parquetFile(HDFS_PATH_DM_CLAIM_ORDER_INFO_ALL).coalesce(1000) 
	claim_order.registerTempTable("claim_order")
	repair_balance = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL).coalesce(1500)
	repair_balance.registerTempTable("repair_balance")
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		dtClaimorder_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_CLAIM_ORDER_INFO).map(lambda x: (x[0:2],(x[2:])))
	except:
		dtClaimorder_temp_rdd = sc.parallelize([])
	claim_rdd = sqlContext.sql("select a.claim_no_id,a.asc_code,a.ro_no,a.vin,a.claim_type,a.gross_credit,a.ro_open_date from claim_order a join repair_balance b on a.asc_code = b.asc_code and a.ro_no=b.ro_no")
	claim_union_rdd = claim_rdd.map(lambda x: (x[0:2],(x[2:]+(today_str,today_str)))) \
						.union(dtClaimorder_temp_rdd).repartition(1000) \
						.reduceByKey(DeduplicationDate) \
						.map(lambda x: (x[0]+x[1])) 
	claim_union_rdd_schemardd = sqlContext.applySchema(claim_union_rdd,dt_claim_order_info_schema)
	claim_union = sqlContext.parquetFile(HDFS_PATH_DT_CLAIM_ORDER_INFO)
	claim_union.registerTempTable("dt_claim_order_info")
	claim_union_rdd_schemardd.insertInto("dt_claim_order_info",overwrite=True)

if __name__ == '__main__':
	getClaimTable(sc,sqlContext)