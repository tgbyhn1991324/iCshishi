# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_DM_DossDolTable_Collector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.table_list.dol_table import *
from mathartsys.supcap.data_mart.table_list.doss_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import Deduplication

#(doss系统)筛选IS_EFF='1'的数据,（dol系统)筛选valid='1'的数据
def doss_dol_configTableGet(sc,sqlContext,today_str):
	#lkup table doss 1:有效数据
	#生成doss经销商基本信息表
	try:
		dossDealer_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_DEALER_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossDealer_temp_rdd = sc.parallelize([])
	tdd_tf = sc.textFile(HDFS_PATH_DOSS_TM_DOSS_DEALER)
	tdd_valid_rdd = tdd_tf.map(lambda x: x.split("\x01"))
	lkup_dealer_rows = tdd_valid_rdd.filter(lambda x:  x[TDD_IS_EFF] == '1') \
									.map(lambda x: (x[TDD_SRC_DEALER_ID],x[TDD_ASC_ID],x[TDD_BRAND_ID],x[TDD_DEALER_CODE],x[TDD_DEALER_ABBRE_NAME],x[TDD_DEALER_NAME],x[TDD_DEALER_STATUS],x[TDD_DEALER_MAIN_CODE],x[TDD_DEALER_GRADE],x[TDD_CHIEF_RESP],x[TDD_PHONE],x[TDD_MOBILE],x[TDD_FAX],x[TDD_EMAIL],x[TDD_POSTCODE],x[TDD_ADDRESS],x[TDD_PARTNER_REGION],x[TDD_COUNTRY_REGION],x[TDD_PROVINCE_ID],x[TDD_CITY_ID],x[TDD_DISTRICT_ID],x[TDD_SHOWROOM_PHONE],x[TDD_SHOWROOM_FAX],x[TDD_SHOWROOM_ADDRESS],x[TDD_IS_VIRTUAL_DEALER],x[TDD_AFFILIATED_DEALER_CODE],x[TDD_DEALER_ORG_ID],x[TDD_PREMIUM_PROD],x[TDD_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
									.union(dossDealer_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_dealer_rows_schemardd = sqlContext.applySchema(lkup_dealer_rows, lkup_doss_dealer_value_dtl_schema)
	lkup_dealer_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_DEALER_VALUE_DTL)
	lkup_dealer_schemardd.registerTempTable("lkup_doss_dealer_value_dtl")
	lkup_dealer_rows_schemardd.insertInto("lkup_doss_dealer_value_dtl",overwrite=True)

	#生成doss颜色基本信息表
	try:
		dossColor_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_COLOR_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossColor_temp_rdd = sc.parallelize([])
	tdc_tf = sc.textFile(HDFS_PATH_DOSS_TM_DOSS_COLOR)
	tdc_valid_rdd = tdc_tf.map(lambda x: x.split("\x01"))
	lkup_color_rows = tdc_valid_rdd.filter(lambda x:  x[TDC_IS_EFF] == '1') \
									.map(lambda x: (x[TDC_SRC_COLOR_ID],x[TDC_COLOR_CODE],x[TDC_ORIGINAL_COLOR_CODE],x[TDC_COLOR_NAME],x[TDC_COLOR_ENGLISH_NAME],x[TDC_LACQUER_TYPE],x[TDC_STATUS],x[TDC_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dossColor_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_color_rows_schemardd = sqlContext.applySchema(lkup_color_rows, lkup_doss_color_value_dtl_schema)
	lkup_color_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_COLOR_VALUE_DTL)
	lkup_color_schemardd.registerTempTable("lkup_doss_color_value_dtl")
	lkup_color_rows_schemardd.insertInto("lkup_doss_color_value_dtl",overwrite=True)

	#生成doss车系基本信息表
	try:
		dossSeries_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_SERIES_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossSeries_temp_rdd = sc.parallelize([])
	tds_tf = sc.textFile(HDFS_PATH_DOSS_TM_DOSS_SERIES)
	tds_valid_rdd = tds_tf.map(lambda x: x.split("\x01"))
	lkup_series_rows = tds_valid_rdd.filter(lambda x:  x[TDS_IS_EFF] == '1') \
									.map(lambda x: (x[TDS_SRC_SERIES_ID],x[TDS_BRAND_ID],x[TDS_SERIES_CODE],x[TDS_SERIES_NAME],x[TDS_SERIES_ENGLISH_NAME],x[TDS_LAUNCH_DATE],x[TDS_FADEOUT_DATE],x[TDS_STATUS],x[TDS_LOAD_FROM],x[TDS_IS_PREMIUM_PROD])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dossSeries_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_series_rows_schemardd = sqlContext.applySchema(lkup_series_rows, lkup_doss_series_value_dtl_schema)
	lkup_series_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_SERIES_VALUE_DTL)
	lkup_series_schemardd.registerTempTable("lkup_doss_series_value_dtl")
	lkup_series_rows_schemardd.insertInto("lkup_doss_series_value_dtl",overwrite=True)

	#生成doss产品基本信息表
	try:
		dossProduct_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PRODUCT_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossProduct_temp_rdd = sc.parallelize([])
	tdpr_tf = sc.textFile(HDFS_PATH_DOSS_TM_DOSS_PRODUCT)
	tdpr_valid_rdd = tdpr_tf.map(lambda x: x.split("\x01"))
	lkup_product_rows = tdpr_valid_rdd.filter(lambda x:  x[TDPR_IS_EFF] == '1') \
									.map(lambda x: (x[TDPR_SRC_PRODUCT_ID],x[TDPR_COLOR_ID],x[TDPR_PACKAGE_ID],x[TDPR_PRODUCT_CODE],x[TDPR_DESCRIPTION],x[TDPR_ENGLISH_NAME],x[TDPR_MATERIAL_NO],x[TDPR_DESIGN_REG_FLAG],x[TDPR_DESIGN_REG_DESC],x[TDPR_LAUNCH_DATE],x[TDPR_FADEOUT_DATE],x[TDPR_MSRP],x[TDPR_STATUS],x[TDPR_CALL_OFF_PRICE],x[TDPR_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dossProduct_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_product_rows_schemardd = sqlContext.applySchema(lkup_product_rows, lkup_doss_product_value_dtl_schema)
	lkup_product_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PRODUCT_VALUE_DTL)
	lkup_product_schemardd.registerTempTable("lkup_doss_product_value_dtl")
	lkup_product_rows_schemardd.insertInto("lkup_doss_product_value_dtl",overwrite=True)

	#生成doss包装配置基本信息表
	try:
		dossPackage_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PACKAGE_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossPackage_temp_rdd = sc.parallelize([])
	tdpa_tf = sc.textFile(HDFS_PATH_DOSS_TM_DOSS_PACKAGE)
	tdpa_valid_rdd = tdpa_tf.map(lambda x: x.split("\x01"))
	lkup_package_rows = tdpa_valid_rdd.filter(lambda x:  x[TDPA_IS_EFF] == '1') \
									.map(lambda x: (x[TDPA_SRC_PACKAGE_ID],x[TDPA_MODEL_ID],x[TDPA_PKG_CODE],x[TDPA_PKG_NAME],x[TDPA_PKG_ENGLISH_NAME],x[TDPA_PKG_DESCRIPTION],x[TDPA_MARKET_NAME],x[TDPA_AD_NAME],x[TDPA_ENGINEERING_NAME],x[TDPA_BODYWORK_TYPE],x[TDPA_DISPLACEMENT],x[TDPA_AIRFEED_TYPE],x[TDPA_TAP_POS_TYPE_DESC],x[TDPA_LOGO_DESC],x[TDPA_TRIM_COLOR],x[TDPA_SEAT_MATERIAL],x[TDPA_HAS_NAVIGATION],x[TDPA_IS_HYBIRD],x[TDPA_HAS_SUNROOF],x[TDPA_HAS_ONSTAR],x[TDPA_PUBLISH_DATE],x[TDPA_LAUNCH_DATE],x[TDPA_FADEOUT_DATE],x[TDPA_STATUS],x[TDPA_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dossPackage_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_package_rows_schemardd = sqlContext.applySchema(lkup_package_rows, lkup_doss_package_value_dtl_schema)
	lkup_package_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PACKAGE_VALUE_DTL)
	lkup_package_schemardd.registerTempTable("lkup_doss_package_value_dtl")
	lkup_package_rows_schemardd.insertInto("lkup_doss_package_value_dtl",overwrite=True)

	#生成doss城市基本信息表
	try:
		dossCity_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_CITY_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dossCity_temp_rdd = sc.parallelize([])
	tcc_tf = sc.textFile(HDFS_PATH_DOSS_TD_COMM_CITY)
	tcc_valid_rdd = tcc_tf.map(lambda x: x.split("\x01"))
	lkup_city_rows = tcc_valid_rdd.map(lambda x: (x[TCC_ID],x[TCC_CITY_CODE],x[TCC_CITY_NAME_CN],x[TCC_PROVINCE_CODE],x[TCC_PROVINCE_NAME_CN],x[TCC_IS_PROVINCE],x[TCC_SRC_ID],x[TCC_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dossCity_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_city_rows_schemardd = sqlContext.applySchema(lkup_city_rows, lkup_doss_city_value_dtl_schema)
	lkup_city_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_CITY_VALUE_DTL)
	lkup_city_schemardd.registerTempTable("lkup_doss_city_value_dtl")
	lkup_city_rows_schemardd.insertInto("lkup_doss_city_value_dtl",overwrite=True)

	#lkup table dol
	#生成dol城市基本信息表
	try:
		dolCity_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CITY_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolCity_temp_rdd = sc.parallelize([])
	tdci_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_CITY)
	tdci_valid_rdd = tdci_tf.map(lambda x: x.split("\x01"))
	lkup_city_rows = tdci_valid_rdd.filter(lambda x:  x[TDCI2_IS_EFF] == '1') \
									.map(lambda x: (x[TDCI2_CITY_ID],x[TDCI2_PROVINCE_ID],x[TDCI2_CITY_NAME],x[TDCI2_LOAD_FROM])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolCity_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_city_rows_schemardd = sqlContext.applySchema(lkup_city_rows, lkup_dol_city_value_dtl_schema).repartition(10)
	lkup_city_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CITY_VALUE_DTL)
	lkup_city_schemardd.registerTempTable("lkup_dol_city_value_dtl")
	lkup_city_rows_schemardd.insertInto("lkup_dol_city_value_dtl",overwrite=True)

	#生成dol颜色基本信息表
	try:
		dolColor_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COLOR_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolColor_temp_rdd = sc.parallelize([])
	tdco_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_COLOR)
	tdco_valid_rdd = tdco_tf.map(lambda x: x.split("\x01"))
	lkup_color_rows = tdco_valid_rdd.filter(lambda x:  x[TDCO2_VALID] == '1') \
									.map(lambda x: (x[TDCO2_COLOR_ID],x[TDCO2_CHINESE_NAME],x[TDCO2_ENGLISH_NAME],x[TDCO2_RPO_CODE],x[TDCO2_STATUS],x[TDCO2_COLOR_CODE],x[TDCO2_EFFECTIVE_DATE],x[TDCO2_END_DATE],x[TDCO2_STATUS_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolColor_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_color_rows_schemardd = sqlContext.applySchema(lkup_color_rows, lkup_dol_color_value_dtl_schema).repartition(10)
	lkup_color_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COLOR_VALUE_DTL)
	lkup_color_schemardd.registerTempTable("lkup_dol_color_value_dtl")
	lkup_color_rows_schemardd.insertInto("lkup_dol_color_value_dtl",overwrite=True)

	#生成dol销售组织基本信息表
	try:
		dolCompany_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COMPANY_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolCompany_temp_rdd = sc.parallelize([])
	tdcm_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_COMPANY)
	tdcm_valid_rdd = tdcm_tf.map(lambda x: x.split("\x01"))
	lkup_company_rows = tdcm_valid_rdd.filter(lambda x:  x[TDCM2_VALID] == '1') \
									.map(lambda x: (x[TDCM2_COMPANY_ID],x[TDCM2_COMPANY_SHORTNAME],x[TDCM2_COMPANY_NAME],x[TDCM2_STATUS],x[TDCM2_PROVINCE_ID],x[TDCM2_OPEN_DATE],x[TDCM2_ADDRESS],x[TDCM2_PHONE],x[TDCM2_PERSON_RESPONSIBLE],x[TDCM2_ZIP_CODE],x[TDCM2_FAX],x[TDCM2_COMPANY_CODE],x[TDCM2_GENDER_ID],x[TDCM2_EFFECTIVE_DATE],x[TDCM2_END_DATE],x[TDCM2_GENDER_DESC],x[TDCM2_STATUS_DESC],x[TDCM2_CITY_ID])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolCompany_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_company_rows_schemardd = sqlContext.applySchema(lkup_company_rows, lkup_dol_company_value_dtl_schema).repartition(10)
	lkup_company_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COMPANY_VALUE_DTL)
	lkup_company_schemardd.registerTempTable("lkup_dol_company_value_dtl")
	lkup_company_rows_schemardd.insertInto("lkup_dol_company_value_dtl",overwrite=True)

	#生成dol销售组织内部部门信息表
	try:
		dolDptorg_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_DPT_ORG_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolDptorg_temp_rdd = sc.parallelize([])
	tddo_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_DPT_ORG)
	tddo_valid_rdd = tddo_tf.map(lambda x: x.split("\x01"))
	lkup_org_rows = tddo_valid_rdd.filter(lambda x:  x[TDDO2_VALID] == '1') \
									.map(lambda x: (x[TDDO2_ORG_ID],x[TDDO2_COMPANY_ID],x[TDDO2_ORG_TYPE_ID],x[TDDO2_ORG_NAME],x[TDDO2_ORG_DESC],x[TDDO2_PHONE],x[TDDO2_MOBILE_PHONE],x[TDDO2_ADDRESS],x[TDDO2_EMAIL],x[TDDO2_PERSON_RESPONSIBLE],x[TDDO2_EFFECTIVE_DATE],x[TDDO2_END_DATE],x[TDDO2_DATA_OWNER],x[TDDO2_PARENT_ORG_ID],x[TDDO2_ORG_ENAME])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolDptorg_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_org_rows_schemardd = sqlContext.applySchema(lkup_org_rows, lkup_dol_dpt_org_value_dtl_schema).repartition(10)
	lkup_org_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_DPT_ORG_VALUE_DTL)
	lkup_org_schemardd.registerTempTable("lkup_dol_org_value_dtl")
	lkup_org_rows_schemardd.insertInto("lkup_dol_org_value_dtl",overwrite=True)

	#生成dol车型基本信息表
	try:
		dolModel_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_MODEL_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolModel_temp_rdd = sc.parallelize([])
	tdmo_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_MODEL)
	tdmo_valid_rdd = tdmo_tf.map(lambda x: x.split("\x01"))
	lkup_model_rows = tdmo_valid_rdd.filter(lambda x:  x[TDMO2_VALID] == '1') \
									.map(lambda x: (x[TDMO2_MODEL_ID],x[TDMO2_MODEL_CODE],x[TDMO2_SERIES_ID],x[TDMO2_CHINESE_NAME],x[TDMO2_ENGLISH_NAME],x[TDMO2_LAUNCH_DATE],x[TDMO2_FACE_OUT_DATE],x[TDMO2_STATUS],x[TDMO2_STATUS_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolModel_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_model_rows_schemardd = sqlContext.applySchema(lkup_model_rows, lkup_dol_model_value_dtl_schema).repartition(10)
	lkup_model_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_MODEL_VALUE_DTL)
	lkup_model_schemardd.registerTempTable("lkup_dol_model_value_dtl")
	lkup_model_rows_schemardd.insertInto("lkup_dol_model_value_dtl",overwrite=True)

	#生成dol包装配置基本信息表
	try:
		dolPackage_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PACKAGE_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolPackage_temp_rdd = sc.parallelize([])
	tdpa_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_PACKAGE)
	tdpa_valid_rdd = tdpa_tf.map(lambda x: x.split("\x01"))
	lkup_package_rows = tdpa_valid_rdd.filter(lambda x:  x[TDPA2_VALID] == '1') \
									.map(lambda x: (x[TDPA2_PACKAGE_ID],x[TDPA2_MODEL_ID],x[TDPA2_CHINESE_NAME],x[TDPA2_ENGLISH_NAME],x[TDPA2_PACKAGE_DESC],x[TDPA2_LAUNCH_DATE],x[TDPA2_FACE_OUT_DATE],x[TDPA2_MARKET_NAME],x[TDPA2_STATUS],x[TDPA2_ENGINEERING_CODE],x[TDPA2_EFFECTIVE_DATE],x[TDPA2_END_DATE],x[TDPA2_STATUS_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolPackage_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_package_rows_schemardd = sqlContext.applySchema(lkup_package_rows, lkup_dol_package_value_dtl_schema).repartition(10)
	lkup_package_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PACKAGE_VALUE_DTL)
	lkup_package_schemardd.registerTempTable("lkup_dol_package_value_dtl")
	lkup_package_rows_schemardd.insertInto("lkup_dol_package_value_dtl",overwrite=True)

	#生成dol产品基本信息表
	try:
		dolProduct_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolProduct_temp_rdd = sc.parallelize([])
	tdpr_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_PRODUCT)
	tdpr_valid_rdd = tdpr_tf.map(lambda x: x.split("\x01"))
	lkup_product_rows = tdpr_valid_rdd.filter(lambda x:  x[TDPR2_VALID] == '1') \
									.map(lambda x: (x[TDPR2_PRODUCT_ID],x[TDPR2_COLOR_ID],x[TDPR2_PACKAGE_ID],x[TDPR2_PRODUCT_DESC],x[TDPR2_STATUS],x[TDPR2_MATERIAL_NUMBER],x[TDPR2_IS_NORMAL],x[TDPR2_LAUNCH_DATE],x[TDPR2_FACE_OUT_DATE],x[TDPR2_CHINESE_NAME],x[TDPR2_ENGLISH_NAME],x[TDPR2_EFFECTIVE_DATE],x[TDPR2_END_DATE],x[TDPR2_STATUS_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolProduct_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_product_rows_schemardd = sqlContext.applySchema(lkup_product_rows, lkup_dol_product_value_dtl_schema).repartition(10)
	lkup_product_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_VALUE_DTL)
	lkup_product_schemardd.registerTempTable("lkup_dol_product_value_dtl")
	lkup_product_rows_schemardd.insertInto("lkup_dol_product_value_dtl",overwrite=True)

	#生成dol产品价格基本信息表
	try:
		dolProductPrice_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_PRICE_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolProductPrice_temp_rdd = sc.parallelize([])
	tdpp_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_PRODUCT_PRICE)
	tdpp_valid_rdd = tdpp_tf.map(lambda x: x.split("\x01"))
	lkup_price_rows = tdpp_valid_rdd.filter(lambda x:  x[TDPP2_VALID] == '1') \
									.map(lambda x: (x[TDPP2_PRICE_ID],x[TDPP2_PRODUCT_ID],x[TDPP2_PRICE_TYPE],x[TDPP2_MATERIAL_NUMBER],x[TDPP2_PRICE],x[TDPP2_LAUNCH_DATE],x[TDPP2_FACE_OUT_DATE],x[TDPP2_EFFECTIVE_DATE],x[TDPP2_END_DATE],x[TDPP2_PRICE_TYPE_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolProductPrice_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_price_rows_schemardd = sqlContext.applySchema(lkup_price_rows, lkup_dol_product_price_value_dtl_schema).repartition(10)
	lkup_price_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_PRICE_VALUE_DTL)
	lkup_price_schemardd.registerTempTable("lkup_dol_price_value_dtl")
	lkup_price_rows_schemardd.insertInto("lkup_dol_price_value_dtl",overwrite=True)

	#生成dol产品价格基本信息表
	try:
		dolProvince_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PROVINCE_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolProvince_temp_rdd = sc.parallelize([])
	tdr_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_PROVINCE)
	tdr_valid_rdd = tdr_tf.map(lambda x: x.split("\x01"))
	lkup_province_rows = tdr_valid_rdd.filter(lambda x:  x[TDR2_IS_EFF] == '1') \
									.map(lambda x: (x[TDR2_PROVINCE_ID],x[TDR2_PROVINCE_NAME],x[TDR2_PROVINCE_ENAME],x[TDR2_LOAD_FROM],x[TDR2_END_DATE],x[TDR2_SMART_NAME],x[TDR2_EFF_DATE])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dolProvince_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_provice_rows_schemardd = sqlContext.applySchema(lkup_province_rows, lkup_dol_province_value_dtl_schema).repartition(10)
	lkup_provice_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PROVINCE_VALUE_DTL)
	lkup_provice_schemardd.registerTempTable("lkup_dol_provice_value_dtl")
	lkup_provice_rows_schemardd.insertInto("lkup_dol_provice_value_dtl",overwrite=True)

	#生成dol车系基本信息表
	try:
		dolSeries_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_SERIES_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolSeries_temp_rdd = sc.parallelize([])
	tds_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_SERIES)
	tds_valid_rdd = tds_tf.map(lambda x: x.split("\x01"))
	lkup_series_rows = tds_valid_rdd.filter(lambda x:  x[TDS2_VALID] == '1') \
									.map(lambda x: (x[TDS2_SERIES_ID],x[TDS2_BRAND_ID],x[TDS2_CHINESE_NAME],x[TDS2_ENGLISH_NAME],x[TDS2_LAUNCH_DATE],x[TDS2_FACE_OUT_DATE],x[TDS2_STATUS],x[TDS2_EFFECTIVE_DATE],x[TDS2_END_DATE],x[TDS2_STATUS_DESC],x[TDS2_IS_IMPORT])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1	))))\
									.union(dolSeries_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	lkup_series_rows_schemardd = sqlContext.applySchema(lkup_series_rows, lkup_dol_series_value_dtl_schema)
	lkup_series_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_SERIES_VALUE_DTL)
	lkup_series_schemardd.registerTempTable("lkup_dol_series_value_dtl")
	lkup_series_rows_schemardd.insertInto("lkup_dol_series_value_dtl",overwrite=True)

def getDolCustomerValueTable(sc,sqlContext,today_str):
	#生成dol客户基本信息表
	try:
		dolCustomer_temp_rdd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CUSTOMER_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolCustomer_temp_rdd = sc.parallelize([])
	tdcu_tf = sc.textFile(HDFS_PATH_DOL_TM_DOL_CUSTOMER)
	tdcu_valid_rdd = tdcu_tf.map(lambda x: x.split("\x01"))
	lkup_customer_rows = tdcu_valid_rdd.filter(lambda x:  x[TDCU2_VALID] == '1') \
									.map(lambda x: (x[TDCU2_CUSTOMER_ID],x[TDCU2_CUSTOMER_TYPE],x[TDCU2_CUSTOMER_STATUS],x[TDCU2_PICKUP],x[TDCU2_PICKUP_PHONE],x[TDCU2_VIN],x[TDCU2_ASC_CODE],x[TDCU2_ORG_ID],x[TDCU2_DUTY_ASC],x[TDCU2_EFFECTIVE_DATE],x[TDCU2_END_DATE],x[TDCU2_CUSTOMER_TYPE_DESC],x[TDCU2_CUSTOMER_STATUS_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1	))))\
									.union(dolCustomer_temp_rdd).repartition(200).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1]))
	lkup_customer_rows_schemardd = sqlContext.applySchema(lkup_customer_rows, lkup_dol_customer_value_dtl_schema).repartition(100)
	lkup_customer_schemardd = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CUSTOMER_VALUE_DTL)
	lkup_customer_schemardd.registerTempTable("lkup_dol_customer_value_dtl")
	lkup_customer_rows_schemardd.insertInto("lkup_dol_customer_value_dtl",overwrite=True)

def getDolSalesOrderTable(sc,sqlContext,today_str):
	#生成dol销售订单信息表
	try:
		dolSalesOrder_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_DEALER_SALES_ORDER).map(lambda x: (x[0],(x[1:] +(-1,)))).coalesce(400)
	except:
		dolSalesOrder_temp_rdd = sc.parallelize([])
	tddso_tf = sc.textFile(HDFS_PATH_DOL_TT_DOL_DEALER_SALES_ORDER)
	tddso_valid_rdd = tddso_tf.map(lambda x: x.split("\x01"))
	dm_order_rows = tddso_valid_rdd.filter(lambda x:  x[TDDSO2_VALID] == '1') \
									.map(lambda x: (x[TDDSO2_SALES_ORDER_ID],x[TDDSO2_ORG_ID],x[TDDSO2_SALES_ORDER_CODE],x[TDDSO2_VEHICLE_ID],x[TDDSO2_CUSTOMER_ID],x[TDDSO2_TOTAL_AMOUNT],x[TDDSO2_PAYMENT_TYPE],x[TDDSO2_INVOICE_DATE],x[TDDSO2_INVOICE_NO],x[TDDSO2_CONTRACT_NUMBER],x[TDDSO2_EFFECTIVE_DATE],x[TDDSO2_END_DATE],x[TDDSO2_PAYMENT_TYPE_DESC])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1	))))\
									.union(dolSalesOrder_temp_rdd).repartition(200).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1]))
	dm_order_rows_schemardd = sqlContext.applySchema(dm_order_rows, dm_dol_dealer_sales_order_schema)
	dm_order_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_DEALER_SALES_ORDER)
	dm_order_schemardd.registerTempTable("dm_dol_order_value_dtl")
	dm_order_rows_schemardd.insertInto("dm_dol_order_value_dtl",overwrite=True)

	# try:
	# 	dolVehicleSale_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_VEHICLE_SALE).map(lambda x: (x[0],(x[1:] +(-1,))))
	# except:
	# 	dolVehicleSale_temp_rdd = sc.parallelize([])
	# tdvs_tf = sc.textFile(HDFS_PATH_DOL_TT_DOL_VEHICLE_SALE)
	# tdvs_valid_rdd = tdvs_tf.map(lambda x: x.split("\x01"))
	# dm_vehicle_rows = tdvs_valid_rdd.filter(lambda x:  x[TDVS2_VALID] == '1' and len(x[2]) == 17 and x[2][0] != '-') \
	# 								.map(lambda x: (x[TDVS2_VEHICLE_ID],x[TDVS2_VN],x[TDVS2_VIN],x[TDVS2_PVI],x[TDVS2_ENGINE_NUMBER],x[TDVS2_TRIM_COLOR],x[TDVS2_TRIM_LEVEL],x[TDVS2_EXTERNAL_COLOR],x[TDVS2_DELIVERY_DATE],x[TDVS2_MILEAGE],x[TDVS2_LICENSE_NUMBER],x[TDVS2_NODE_DATE],x[TDVS2_VHCL_NODE],x[TDVS2_LOCATION],x[TDVS2_SCHEDULE_DATE],x[TDVS2_MODEL_YEAR],x[TDVS2_LIFE_CYCLE],x[TDVS2_ATTACHMENT],x[TDVS2_NODE_STATUS],x[TDVS2_HOLD_STATUS],x[TDVS2_INIT_VDC],x[TDVS2_IS_VIP],x[TDVS2_VIP_COMMENT],x[TDVS2_VIP_DATE],x[TDVS2_VN_CREATE],x[TDVS2_VHCL_TYPE],x[TDVS2_CUSTOMER_ID],x[TDVS2_ORG_ID],x[TDVS2_BRAND_ID],x[TDVS2_CORP_ID],x[TDVS2_EFFECTIVE_DATE],x[TDVS2_END_DATE],x[TDVS2_VHCL_NODE_DESC],x[TDVS2_HOLD_STATUS_DESC],x[TDVS2_VHCL_TYPE_DESC],x[TDVS2_LIFE_CYCLE_DESC],x[TDVS2_PRODUCT_ID])) \
	# 								.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1	))))\
	# 								.union(dolVehicleSale_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
	# 								.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(100)
	# dm_vehicle_rows_schemardd = sqlContext.applySchema(dm_vehicle_rows, dm_dol_vehicle_sale_schema)
	# dm_vehicle_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_VEHICLE_SALE)
	# dm_vehicle_schemardd.registerTempTable("dm_dol_vehicle_value_dtl")
	# dm_vehicle_rows_schemardd.insertInto("dm_dol_vehicle_value_dtl",overwrite=True)
def getDossVehicleValueTable(sc,sqlContext,today_str):
	#DM
	#生成doss车辆基本信息表
	try:
		dmVehicle_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_VALUE_DTL).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dmVehicle_temp_rdd = sc.parallelize([])
	tdv_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_VEHICLE)
	dvvd_pf = tdv_tf.map(lambda x: x.split("\x01"))
	dm_vehicle_rows = dvvd_pf.filter(lambda x:  len(x[TDV_VIN]) == 17) \
									.map(lambda x: (x[TDV_VEHICLE_ID],x[TDV_BRAND_ID],x[TDV_SERIES_ID],x[TDV_MODEL_ID],x[TDV_PACKAGE_ID],x[TDV_COLOR_ID],x[TDV_PRODUCT_ID],x[TDV_LOCATION_ID],x[TDV_DEALER_ID],x[TDV_VIN],x[TDV_CALL_OFF_DATE],x[TDV_ENGINE_NO],x[TDV_MANUFACTURE_DATE],x[TDV_INVOICE_DATE],x[TDV_STATUS],x[TDV_CAR_LICENSE_NO],x[TDV_QUALITY_ISSUE_STATUS],x[TDV_CALL_OFF_NO],x[TDV_TEST_DRIVING_FLAG],x[TDV_FACTORY_DATE],x[TDV_IS_DIRECT_SALE],x[TDV_PAYMENT_METHOD],x[TDV_RETAIL_DATE],x[TDV_LOAD_FROM],x[TDV_CALL_OFF_ORG_CODE])) \
									.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1))))\
									.union(dmVehicle_temp_rdd).repartition(200).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
									.map(lambda x: ((x[0],)+x[1][0:-1]))
	dm_vehicle_rows_schemardd = sqlContext.applySchema(dm_vehicle_rows, dm_vehicle_value_dtl_schema)
	dm_vehicle_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_VALUE_DTL)
	dm_vehicle_schemardd.registerTempTable("dm_doss_vehicle_value_dtl")
	dm_vehicle_rows_schemardd.insertInto("dm_doss_vehicle_value_dtl",overwrite=True)

if __name__ == '__main__':
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	getDossVehicleValueTable(sc,sqlContext,today_str)
	getDolCustomerValueTable(sc,sqlContext,today_str)
	getDolSalesOrderTable(sc,sqlContext,today_str)
	doss_dol_configTableGet(sc,sqlContext,today_str)