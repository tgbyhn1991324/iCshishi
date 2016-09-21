# -*- coding: utf-8 -*-
import sys 
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_TableInit")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *              

def initParquetFile(schema,path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	#umt手动维护表
	# try:
	# 	initParquetFile(umt_tag_value_dtl_schema, HDFS_PATH_UMT_TAG_VALUE_DTL)
	# except Exception, e:
	# 	print "initParquetFile UMT_TAG_VALUE_DTL fail" +str(e)

	#lkup跟随系统改变的配置表(doss)
	try:
		initParquetFile(lkup_doss_dealer_value_dtl_schema, HDFS_PATH_LKUP_DOSS_DEALER_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_DEALER_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_doss_color_value_dtl_schema, HDFS_PATH_LKUP_DOSS_COLOR_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_COLOR_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_doss_series_value_dtl_schema, HDFS_PATH_LKUP_DOSS_SERIES_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_SERIES_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_doss_product_value_dtl_schema, HDFS_PATH_LKUP_DOSS_PRODUCT_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_PRODUCT_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_doss_package_value_dtl_schema, HDFS_PATH_LKUP_DOSS_PACKAGE_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_PACKAGE_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_doss_city_value_dtl_schema, HDFS_PATH_LKUP_DOSS_CITY_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOSS_CITY_VALUE_DTL fail" +str(e)
	# try:
	# 	initParquetFile(lkup_maintain_value_dtl_schema, HDFS_PATH_LKUP_MAINTAIN_VALUE_DTL)
	# except Exception, e:
	# 	print "initParquetFile LKUP_MAINTAIN_VALUE_DTL fail" +str(e)
	#lkup跟随系统改变的配置表(dol)
	try:
		initParquetFile(lkup_dol_city_value_dtl_schema, HDFS_PATH_LKUP_DOL_CITY_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_CITY_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_color_value_dtl_schema, HDFS_PATH_LKUP_DOL_COLOR_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_COLOR_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_company_value_dtl_schema, HDFS_PATH_LKUP_DOL_COMPANY_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_COMPANY_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_dpt_org_value_dtl_schema, HDFS_PATH_LKUP_DOL_DPT_ORG_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_DPT_ORG_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_model_value_dtl_schema, HDFS_PATH_LKUP_DOL_MODEL_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_MODEL_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_package_value_dtl_schema, HDFS_PATH_LKUP_DOL_PACKAGE_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_PACKAGE_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_product_value_dtl_schema, HDFS_PATH_LKUP_DOL_PRODUCT_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_PRODUCT_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_product_price_value_dtl_schema, HDFS_PATH_LKUP_DOL_PRODUCT_PRICE_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_PRODUCT_PRICE_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_province_value_dtl_schema, HDFS_PATH_LKUP_DOL_PROVINCE_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_PROVINCE_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_series_value_dtl_schema, HDFS_PATH_LKUP_DOL_SERIES_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_SERIES_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(lkup_dol_customer_value_dtl_schema, HDFS_PATH_LKUP_DOL_CUSTOMER_VALUE_DTL)
	except Exception, e:
		print "initParquetFile LKUP_DOL_CUSTOMER_VALUE_DTL fail" +str(e)

	#dm中间层数据表
	try:
		initParquetFile(dm_dol_dealer_sales_order_schema, HDFS_PATH_DM_DOL_DEALER_SALES_ORDER)
	except Exception, e:
		print "initParquetFile DM_DOL_DEALER_SALES_ORDER fail" +str(e)
	try:
		initParquetFile(dm_dol_vehicle_sale_schema, HDFS_PATH_DM_DOL_VEHICLE_SALE)
	except Exception, e:
		print "initParquetFile DM_DOL_VEHICLE_SALE fail" +str(e)
	try:
		initParquetFile(dolSubDossVin_schema, HDFS_PATH_DM_DOL_SUB_DOSS_VIN)
	except Exception, e:
		print "initParquetFile DM_DOL_VEHICLE_SALE fail" +str(e)
	try:
		initParquetFile(dm_vehicle_value_dtl_schema, HDFS_PATH_DM_VEHICLE_VALUE_DTL)
	except Exception, e:
		print "initParquetFile DM_VEHICLE_VALUE_DTL fail" +str(e)
	try:
		initParquetFile(dm_vehicle_info_a_schema, HDFS_PATH_DM_VEHICLE_INFO_A)
	except Exception, e:
		print "initParquetFile DM_VEHICLE_INFO_A fail" +str(e)
	try:
		initParquetFile(dm_vehicle_info_b_schema, HDFS_PATH_DM_VEHICLE_INFO_B)
	except Exception, e:
		print "initParquetFile DM_VEHICLE_INFO_B fail" +str(e)
	try:
		initParquetFile(dm_balance_order_info_schema, HDFS_PATH_DM_BALANCE_ORDER_INFO)
	except Exception, e:
		print "initParquetFile DM_BALANCE_ORDER_INFO fail" +str(e)
	try:
		initParquetFile(dm_repair_order_info_schema, HDFS_PATH_DM_REPAIR_ORDER_INFO)
	except Exception, e:
		print "initParquetFile DM_REPAIR_ORDER_INFO fail" +str(e)
	try:
		initParquetFile(dm_bo_repair_part_info_schema, HDFS_PATH_DM_BO_REPAIR_PART_INFO)
	except Exception, e:
		print "initParquetFile DM_BO_REPAIR_PART_INFO fail" +str(e)
	try:
		initParquetFile(dm_bo_sale_part_info_schema, HDFS_PATH_DM_BO_SALE_PART_INFO)
	except Exception, e:
		print "initParquetFile DM_BO_SALE_PART_INFO fail" +str(e)
	try:
		initParquetFile(dm_claim_order_info_all_schema, HDFS_PATH_DM_CLAIM_ORDER_INFO_ALL)
	except Exception, e:
		print "initParquetFile DM_CLAIM_ORDER_INFO_ALL fail" +str(e)
	try:
		initParquetFile(dm_order_upload_track_doss_schema, HDFS_PATH_DM_ORDER_UPLOAD_TRACK_DOSS)
	except Exception, e:
		print "initParquetFile DM_CLAIM_ORDER_INFO_ALL fail" +str(e)

	#dt结果数据表，可直接用于应用开发
	try:
		initParquetFile(dt_customer_info_cem_schema, HDFS_PATH_DT_CUSTOMER_INFO_CEM)
	except Exception, e:
		print "initParquetFile DT_CUSTOMER_INFO_CEM fail" +str(e)
	try:
		initParquetFile(dt_customer_info_idm_schema, HDFS_PATH_DT_CUSTOMER_INFO_IDM)
	except Exception, e:
		print "initParquetFile DT_CUSTOMER_INFO_IDM fail" +str(e)
	try:
		initParquetFile(dt_vehicle_info_total_schema, HDFS_PATH_DT_VEHICLE_INFO_TOTAL)
	except Exception, e:
		print "initParquetFile DT_VEHICLE_INFO_TOTAL fail" +str(e)
	try:
		initParquetFile(dt_repair_order_total_schema, HDFS_PATH_DT_REPAIR_ORDER_TOTAL)  
	except Exception, e:
		print "initParquetFile DT_REPAIR_ORDER_TOTAL fail" +str(e)
	try:
		initParquetFile(dt_bo_repair_part_total_schema, HDFS_PATH_DT_BO_REPAIR_PART_TOTAL)  
	except Exception, e:
		print "initParquetFile DT_BO_REPAIR_PART_TOTAL fail" +str(e)
	try:
		initParquetFile(dt_claim_order_info_schema, HDFS_PATH_DT_CLAIM_ORDER_INFO)
	except Exception, e:
		print "initParquetFile DT_CLAIM_ORDER_INFO fail" +str(e)
	try:
		initParquetFile(dt_showroom_consult_info_schema, HDFS_PATH_DT_SHOWROOM_CONSULT_INFO)
	except Exception, e:
		print "initParquetFile DT_SHOWROOM_CONSULT_INFO fail" +str(e)
	try:
		initParquetFile(dt_call_consult_info_schema, HDFS_PATH_DT_CALL_CONSULT_INFO)
	except Exception, e:
		print "initParquetFile DT_CALL_CONSULT_INFO fail" +str(e)
	try:
		initParquetFile(dt_marketing_activity_ind_schema, HDFS_PATH_DT_MARKETING_ACTIVITY_IND)
	except Exception, e:
		print "initParquetFile DT_MARKETING_ACTIVITY_IND fail" +str(e)
	try:
		initParquetFile(dt_marketing_activity_org_schema, HDFS_PATH_DT_MARKETING_ACTIVITY_ORG)
	except Exception, e:
		print "initParquetFile DT_MARKETING_ACTIVITY_ORG fail" +str(e)
	try:
		initParquetFile(dt_test_driving_info_schema, HDFS_PATH_DT_TEST_DRIVING_INFO)
	except Exception, e:
		print "initParquetFile DT_TEST_DRIVING_INFO fail" +str(e)
	try:
		initParquetFile(dt_call_comm_info_schema, HDFS_PATH_DT_CALL_COMM_INFO)
	except Exception, e:
		print "initParquetFile DT_CALL_COMM_INFO fail" +str(e)
