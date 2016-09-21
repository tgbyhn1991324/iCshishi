# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDmBalanceOrderTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import getRDDfromTextFilePartitionByDT,DeduplicationDt

#筛选表TT_ASC_BALANCE_ORDER表中IS_RED！='1'and (LAST_BALANCE_NO=='null'或不存在)的数据
#根据key值(ASC_CODE,BALANCE_NO,VIN)reduceByKey,取最新dt记录
def getTempBalanceOrder(sc,sqlContext,dt,interval=0):
	try:
		dmBalanceorder_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_BALANCE_ORDER_INFO).map(lambda x: (x[0:3],(x[3:-3]+(0,'null')+x[-3:])))
	except:
		dmBalanceorder_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	tt_asc_balance_order_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DATASTORE_TT_ASC_BALANCE_ORDER,dt,interval) \
								.map(lambda x: ((x[B_ASC_CODE],x[B_BALANCE_NO],x[B_VIN]),(x[B_RO_NO],x[B_PLANT_NO],x[B_BALANCE_MODE_CODE],x[B_BALANCE_MODE_NAME],x[B_LICENSE],x[B_ENGINE_NO],x[B_PAYMENT_TYPE],x[B_PAY_NAME],x[B_LABOUR_AMOUNT],x[B_LABOUR_AMOUNT_DISCOUNT],x[B_REPAIR_PART_AMOUNT],x[B_REPAIR_PART_DISCOUNT],x[B_SALE_PART_AMOUNT],x[B_SALE_PART_DISCOUNT],x[B_ADD_ITEM_AMOUNT],x[B_ADD_ITEM_DISCOUNT],x[B_OVER_ITEM_AMOUNT],x[B_OVER_ITEM_DISCOUNT],x[B_NET_AMOUNT],x[B_TAX],x[B_TOTAL_AMOUNT],x[B_ZERO_BALANCED],x[B_RECEIVE_AMOUNT],x[B_REPAIR_PART_COST],x[B_SALE_PART_COST],x[B_DERATE_AMOUNT],x[B_PAYOFF_TAG],x[B_IS_VISIABLE],x[B_SGM_WRT_TAG],x[B_REPAIR_CAMPGN_TAG],x[B_IS_WRT_CLOSED],x[B_SGM_VIN_TAG],x[B_CAMPAIGN_WARRANTY_TAG],x[B_BALANCE_TIME],x[B_SQUARE_DATE],x[B_PRODUCTION_VALUE],x[B_LABOUR_COST],x[B_INVOICE_NO_PAY],x[B_REPAIR_TYPE],x[B_RO_TYPE],x[B_DISCOUNT_MODE_CODE],x[B_REPAIR_TYPE_DESC],x[B_RO_TYPE_DESC], \
								x[B_IS_RED],x[B_LAST_BALANCE_NO],x[B_DT],today_str,today_str))) \
								.union(dmBalanceorder_temp_rdd).coalesce(2000) \
								.reduceByKey(DeduplicationDt) \
								.filter(lambda x: x[1][-5] != '1' and (x[1][-4] == 'null' or not x[1][-4])) \
								.map(lambda x: (x[0]+x[1][0:-5]+x[1][-3:])) 
	tt_asc_balance_order_schemardd = sqlContext.applySchema(tt_asc_balance_order_rdd,dm_balance_order_info_schema)
	tt_asc_balance_order = sqlContext.parquetFile(HDFS_PATH_DM_BALANCE_ORDER_INFO)
	tt_asc_balance_order.registerTempTable("tt_asc_balance_order_all")				  
	tt_asc_balance_order_schemardd.insertInto("tt_asc_balance_order_all",overwrite=True)

if __name__ == '__main__':
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getTempBalanceOrder(sc,sqlContext,dt,interval)
