# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDmPartTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import getRDDfromTextFilePartitionByDT,DeduplicationDt

#筛选TT_ASC_BO_REPAIR_PART表中PART_SALE_AMOUNT != 'null' and PART_QUANTITY != 'null' and PART_NO in("93736042","93736303","93736304","93730361","93732554","12345104","88861011","93735888","93735928","93736473","93736594","90799402")
#根据key值（repair_part_id,part_no）reduceByKey,取最新dt记录
def getTempRepairPart(sc,sqlContext,dt,interval=0):
	try:
		dmRepairpart_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_REPAIR_PART_INFO).map(lambda x: (x[0:2],x[2:]))
	except:
		dmRepairpart_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	tt_asc_bo_repair_part_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DATASTORE_TT_ASC_BO_REPAIR_PART,dt,interval) \
								.filter(lambda x: x[RP_PART_SALE_AMOUNT] != 'null' and x[RP_PART_QUANTITY] != 'null' and x[RP_PART_NO] in PARTKEY) \
								.map(lambda x: ((x[RP_REPAIR_PART_ID],x[RP_PART_NO]),(x[RP_ASC_CODE],x[RP_RO_NO],x[RP_VIN],x[RP_BALANCE_NO],x[RP_SRC_REPAIR_ID],x[RP_STORAGE_CODE],x[RP_STORAGE_POSITION],x[RP_IS_MAIN_PART],x[RP_PART_NAME],x[RP_PART_QUANTITY],x[RP_UNIT],x[RP_PRICE_TYPE],x[RP_PRICE_TYPE_DESC],x[RP_PRICE_RATE],x[RP_SGM_LIMIT_PRICE],x[RP_PART_COST_PRICE],x[RP_PART_SALE_PRICE],x[RP_PART_COST_AMOUNT],x[RP_PART_SALE_AMOUNT],x[RP_ACCOUNT_TAG],x[RP_BATCH_NO],x[RP_SGM_TAG],x[RP_OUT_STOCK_NO],x[RP_BO_ID],x[RP_SGM_VIN_TAG],x[RP_PART_ID],x[RP_DT]))) \
								.map(lambda x: (x[0],(x[1] +(today_str,today_str)))) \
								.union(dmRepairpart_temp_rdd).repartition(500) \
								.reduceByKey(DeduplicationDt) \
								.map(lambda x: (x[0]+x[1])) 
	tt_asc_bo_repair_part_rdd_schemardd = sqlContext.applySchema(tt_asc_bo_repair_part_rdd,dm_bo_repair_part_info_schema)
	repair_part_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_REPAIR_PART_INFO)
	repair_part_rdd.registerTempTable("dm_bo_repair_part_info")				  
	tt_asc_bo_repair_part_rdd_schemardd.insertInto("dm_bo_repair_part_info",overwrite=True)

#筛选TT_ASC_BO_SALE_PART表中PART_SALE_AMOUNT != 'null' and PART_QUANTITY != 'null' and PART_NO in("93736042","93736303","93736304","93730361","93732554","12345104","88861011","93735888","93735928","93736473","93736594","90799402")
#根据key值（sale_part_id,asc_code）reduceByKey,取最新dt记录
def getTempSalePart(sc,sqlContext,dt,interval=0):
	try:
		dmSalepart_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_SALE_PART_INFO).map(lambda x: ((x.sale_part_id,x.asc_code),((x.part_no,)+x[3:])))
	except:
		dmSalepart_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	tt_asc_bo_sale_part_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DATASTORE_TT_ASC_BO_SALE_PART,dt,interval) \
								.filter(lambda x: x[SP_PART_SALE_AMOUNT] != 'null' and x[SP_PART_QUANTITY] != 'null' and x[SP_PART_NO] in PARTKEY) \
								.map(lambda x: ((x[SP_SALE_PART_ID],x[SP_ASC_CODE]),(x[SP_PART_NO],x[SP_RO_NO],x[SP_VIN],x[SP_BALANCE_NO],x[SP_SGM_VIN_TAG],x[SP_PART_ID],x[SP_SRC_SALE_PART_ID],x[SP_STORAGE_CODE],x[SP_STORAGE_POSITION],x[SP_PART_NAME],x[SP_PART_QUANTITY],x[SP_UNIT],x[SP_PRICE_TYPE],x[SP_PRICE_RATE],x[SP_SGM_LIMIT_PRICE],x[SP_PART_COST_PRICE],x[SP_PART_SALE_PRICE],x[SP_PART_COST_AMOUNT],x[SP_PART_SALE_AMOUNT],x[SP_BATCH_NO],x[SP_OUT_STOCK_NO],x[SP_SYSTEM_TAG],x[SP_ITEM_TYPE],x[SP_BO_ID],x[SP_DT]))) \
								.map(lambda x: (x[0],(x[1]+(today_str,today_str)))) \
								.union(dmSalepart_temp_rdd).repartition(100) \
								.reduceByKey(DeduplicationDt) \
								.map(lambda x: ((x[0][0],x[1][0],x[0][1])+x[1][1:])) 
	tt_asc_bo_sale_part_rdd_schemardd = sqlContext.applySchema(tt_asc_bo_sale_part_rdd,dm_bo_sale_part_info_schema)
	sale_part_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BO_SALE_PART_INFO)
	sale_part_rdd.registerTempTable("dm_bo_sale_part_info")				  
	tt_asc_bo_sale_part_rdd_schemardd.insertInto("dm_bo_sale_part_info",overwrite=True)

if __name__ == '__main__':
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getTempRepairPart(sc,sqlContext,dt,interval)
	getTempSalePart(sc,sqlContext,dt,interval)
