# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDmRepairOrderTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import getRDDfromTextFilePartitionByDT,DeduplicationDt

#TT_ASC_REPAIR_ORDER表中筛选START_TIME != 'null'，根据key值（ASC_CODE,RO_NO,VIN）reduceByKey,取最新dt记录
def getTempRepairOrder(sc,sqlContext,dt,interval=0):
	try:
		dmRepairorder_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_REPAIR_ORDER_INFO).map(lambda x: (x[0:3],x[3:]))
	except:
		dmRepairorder_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	tt_asc_repair_order_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DATASTORE_TT_ASC_REPAIR_ORDER,dt,interval) \
							.filter(lambda x: x[R_START_TIME] != 'null') \
							.map(lambda x: ((x[R_ASC_CODE],x[R_RO_NO],x[R_VIN]),(x[R_BALANCE_NO],x[R_LAST_BALANCE_NO],x[R_RO_TYPE],x[R_SERVICE_ADVISOR],x[R_START_TIME],x[R_OWNER_NO],x[R_OWNER_NAME],x[R_OWNER_PROPERTY],x[R_LICENSE],x[R_ENGINE_NO],x[R_IN_MILEAGE],x[R_OUT_MILEAGE],x[R_DELIVERER_PHONE],x[R_DELIVERER_MOBILE],x[R_DELIVERY_TAG],x[R_DELIVERY_DATE],x[R_IS_CHANGE_MILEAGE],x[R_REMARK],x[R_COMPAIGN_WARRANTY_TAG],x[R_SGM_WRT_TAG],x[R_IS_WRT_CLOSED],x[R_TRACING_TAG],x[R_CHANGE_MILEAGE],x[R_IS_WASH],x[R_TEST_DRIVER],x[R_IS_PRE_PRINT],x[R_FINISH_USER],x[R_CLAIM_NO],x[R_IS_PRE_SALE],x[R_IS_VISIABLE],x[R_SUIT_TYPE],x[R_SUIT_AMOUNT],x[R_CHARGE_TYPE],x[R_SGM_VIN_TAG],x[R_BALANCE_MODE_CODE],x[R_DISCOUNT_MODE],x[R_IS_DELETE],x[R_END_TIME_SUPPOSED_TIMESTAMP],x[R_COMPLETE_TIME_TIMESTAMP],x[R_DELIVERY_DATE_TIMESTAMP],x[R_ORDER_TAG],x[R_REPAIR_TYPE],int(x[R_DT])))) \
							.map(lambda x: (x[0],(x[1][:] +(today_str,today_str)))) \
							.union(dmRepairorder_temp_rdd).repartition(2000) \
							.reduceByKey(DeduplicationDt) \
							.map(lambda x: (x[0]+x[1])) 
	tt_asc_repair_order_schemardd = sqlContext.applySchema(tt_asc_repair_order_rdd,dm_repair_order_info_schema)
	tt_asc_repair_order = sqlContext.parquetFile(HDFS_PATH_DM_REPAIR_ORDER_INFO)
	tt_asc_repair_order.registerTempTable("dm_repair_order_info")				  
	tt_asc_repair_order_schemardd.insertInto("dm_repair_order_info",overwrite=True)

if __name__ == '__main__':
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getTempRepairOrder(sc,sqlContext,dt,interval)