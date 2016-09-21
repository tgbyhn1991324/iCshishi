# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreDmClaimOrderTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import getRDDfromTextFilePartitionByDT,DeduplicationDt

#表TT_ASC_CLAIM_ORDER中RO_OPEN_DATE not in ('null','Null','NULL') and 
#(CLAIM_TYPE] in('FO','GW','08','ZFAT','ZSSP') or CLAIM_TYPE in ('02','01','07','ZREG'))，
#根据key值(CLAIM_NO_ID,ASC_CODE)reduceByKey,取最新dt记录
def getTempClaimOrder(sc,sqlContext,dt,interval=0):
	try:
		dmClaimorder_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_CLAIM_ORDER_INFO_ALL).map(lambda x: (x[0:2],x[2:]))
	except:
		dmClaimorder_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	tt_asc_claim_order_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DATASTORE_TT_ASC_CLAIM_ORDER,dt,interval) \
							   .filter(lambda x: (x[C_RO_OPEN_DATE] not in ('null','Null','NULL') and (x[C_CLAIM_TYPE] in CLAIMWARRANTYLIST or x[C_CLAIM_TYPE] in CLAIMGOODWILLLIST))) \
							   .map(lambda x: ((x[C_CLAIM_NO_ID],x[C_ASC_CODE]),(x[C_RO_NO],x[C_VIN],x[C_BALANCE_NO],x[C_CLAIM_NO],x[C_LINE_NO],x[C_TROUBLE_DESC],x[C_CLAIM_TYPE],x[C_APPROVAL_CODE],x[C_EID],x[C_PRODUCT_LINE],x[C_BASE_LABOUR_PRICE],x[C_SPECIAL_LABOUR_PRICE],x[C_NET_ITEM_LIMIT],x[C_VAT_RATE],x[C_PARTS_ALLOWANCE_RATE],x[C_ADD_HOUR_LIMIT],x[C_TROUBLE_CODE],x[C_TROUBLE_REASON],x[C_OBDII_CODE],x[C_COMPLAIN_CODE],x[C_DAMAGE_TYPE],x[C_DAMAGE_AREA],x[C_DAMAGE_DEGREE],x[C_LABOUR_OPERATION_CODE],x[C_LABOUR_OPERATION_DESC],x[C_LABOUR_HOURS],x[C_ADDITIONAL_HOUR],x[C_OTHER_HOURS],x[C_LABOUR_HOUR_PRICE],x[C_ADD_LABOUR_PRICE],x[C_OTHER_LABOUR_PRICE],x[C_STD_LABOUR_AMOUNT],x[C_ADD_LABOUR_AMOUNT],x[C_OTHER_LABOUR_AMOUNT],x[C_BEGIN_TIME],x[C_END_TIME],x[C_RO_OPEN_DATE],x[C_RO_CLOSE_DATE],x[C_FAILED_PART_NO],x[C_FAILED_PART_NAME],x[C_FAILED_PART_COUNT],x[C_LABOUR_AMOUNT],x[C_PARTS_AMOUNT],x[C_NET_ITEM_AMOUNT],x[C_NET_ITEM_TYPE],x[C_REPAIR_TOTAL],x[C_TAX_AMOUNT],x[C_GROSS_CREDIT],x[C_LICENSE],x[C_MODEL_YEAR],x[C_ENGINE_NO],x[C_MILEAGE],x[C_WRT_BEGIN_MILEAGE],x[C_WRT_BEGIN_DATE],x[C_RETURN_DENIAL_REASON],x[C_CYCLE_NO],x[C_APPLY_TIMES],x[C_CLAIM_TIMES],x[C_CAMPAIGN_NO],x[C_CAMPAIGN_TAG],x[C_APPLY_DATE],x[C_REDUCT_DEBIT_TAG],x[C_CLAIM_STATUS],x[C_PROCESS_STATUS],x[C_RETURN_DATE],x[C_DOWN_STAMP],x[C_PLANT_NO],x[C_PAY_AMOUNT],x[C_PAY_DATE],x[C_CORRECTION_DESC],x[C_CLAIM_REMARK],x[C_DOWN_STAMP_MICRO],x[C_GWM_CLAIM_NO],x[C_GWM_CLAIM_VERSION_NO],x[C_COMPLAINT_CATEGORY],x[C_COMPLAINT_DESC],x[C_LABOR_NARRATIVE_TYPE],x[C_SPARE_PART_DATE],x[C_SPARE_PART_INVOICE],x[C_SPARE_PART_MILEAGE],x[C_ADDITIONAL_CREDIT],x[C_DEBIT_IND],x[C_ADJ_REASON_CODE],x[C_AUTH_REQUEST_IND],x[C_AUTH_REQUEST_REASON],x[C_AUTH_CODE],x[C_AUTH_REASON],x[C_CLAIM_CATEGORY],x[C_TRANSP_CARRIER],x[C_RECEIPT_NO],x[C_VEHICLE_ARRIVAL_DATE],x[C_DAMAGE_CODE],x[C_REPAIR_ITEM_ID],x[C_LABOR_OPERATION_COMMENT],x[C_PRE_CLAIM_IND],x[C_PRE_CLAIM_STATUS],x[C_PRE_CLAIM_NO],x[C_VEHICLE_PROPERTY],x[C_SUBSECTN_ID],x[C_CLAIM_CAMPAIGN_ID],x[C_SGM_FLAG],x[C_ZTPT_FLAG],x[C_PART_HANDLING_AMOUNT],x[C_IS_EDIT],x[C_UPC],x[C_FNA],x[C_CASE_NO],x[C_CHECK_NO],x[C_LOAD_FROM],x[C_DT]))) \
							   .map(lambda x: (x[0],(x[1][:]+(today_str,today_str)))) \
							   .union(dmClaimorder_temp_rdd).repartition(1500) \
							   .reduceByKey(DeduplicationDt) \
							   .map(lambda x: (x[0]+x[1])) 
	tt_asc_claim_order_schemardd = sqlContext.applySchema(tt_asc_claim_order_rdd,dm_claim_order_info_all_schema)
	tt_asc_claim_order = sqlContext.parquetFile(HDFS_PATH_DM_CLAIM_ORDER_INFO_ALL)
	tt_asc_claim_order.registerTempTable("dm_claim_order_info_all")				  
	tt_asc_claim_order_schemardd.insertInto("dm_claim_order_info_all",overwrite=True)

if __name__ == '__main__':
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getTempClaimOrder(sc,sqlContext,dt,interval)