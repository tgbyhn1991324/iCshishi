# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_PhoneRIDTableCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.table_list.doss_tt_doss_dcc_call_record_table import *
from mathartsys.supcap.data_mart.table_list.doss_tt_doss_prospect_ind_table import *
from mathartsys.supcap.data_mart.table_list.doss_tt_doss_prospect_org_table import *
from mathartsys.supcap.data_mart.table_list.cac_s_srv_req_table import *
from mathartsys.supcap.data_mart.table_list.doss_tt_doss_showroom_flow_table import *
from mathartsys.supcap.data_mart.table_list.doss_tt_doss_test_driving_table import *
from mathartsys.supcap.data_mart.table_list.cem_cx_asset_tt_table import *
from mathartsys.supcap.data_mart.table_list.doss_tt_doss_order_upload_track_table import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import getRightPhoneNum,listChgRow,getIdmTags,Deduplication,getRDDfromTextFilePartitionByDT,DeduplicationDt

#生成电话沟通行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def cacCallTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		csr_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_CALL_COMM_INFO).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		csr_temp_rdd = sc.parallelize([])
	csr_tf = sc.textFile(HDFS_PATH_CAC_S_SRV_REQ)
	csr_valid_rdd = csr_tf.map(lambda x: x.split("\x01")) \
							.filter(lambda x: getRightPhoneNum((x[CAC_ALT_CON_PH_NUM],'1')) == 0 and x[CAC_CREATED] not in ('null','NULL','Null')) \
							.map(lambda x:(x[CAC_ROW_ID],x[CAC_CREATED],x[CAC_LAST_UPD],x[CAC_SR_NUM],x[CAC_ACT_CLOSE_DT],x[CAC_ALT_CON_PH_NUM],x[CAC_CST_CON_ID],x[CAC_CST_OU_ID],x[CAC_DESC_TEXT],x[CAC_OWNER_EMP_ID],x[CAC_SR_AREA],x[CAC_SR_CAT_TYPE_CD],x[CAC_SR_PRIO_CD],x[CAC_SR_SEV_CD],x[CAC_SR_STAT_ID],x[CAC_SR_SUBTYPE_CD],x[CAC_SR_SUB_AREA],x[CAC_SR_SUB_STAT_ID],x[CAC_SR_TITLE],x[CAC_X_CAC_BRAND],x[CAC_X_CAC_LCNS_NUM],x[CAC_X_CREATED],x[CAC_X_RSA_SR_ID],x[CAC_X_RSA_TYPE],x[CAC_X_SERIES])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(csr_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(100)	
	rows_schemardd = sqlContext.applySchema(csr_valid_rdd,dt_call_comm_info_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_CALL_COMM_INFO)
	schemardd.registerTempTable("DT_CALL_COMM_INFO")
	rows_schemardd.insertInto("DT_CALL_COMM_INFO",overwrite=True)

#生成试驾行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def dossTestDrivingTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		tdtd_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_TEST_DRIVING_INFO).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		tdtd_temp_rdd = sc.parallelize([])
	tdtd_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_TEST_DRIVING)
	tdtd_valid_rdd = tdtd_tf.map(lambda x: x.split("\x01")) \
							.filter(lambda x: getRightPhoneNum((x[TDTD_TEST_DRIVING_MOBILE],'1')) == 0 and x[TDTD_TEST_DRIVING_DATE] not in ('null','NULL','Null')) \
							.map(lambda x:(x[TDTD_TEST_DRIVING_ID],x[TDTD_BRAND_ID],x[TDTD_DEALER_ID],x[TDTD_TEST_DRIVING_ROUTE_ID],x[TDTD_OPP_ID],x[TDTD_TEST_DRIVING_DATE],x[TDTD_START_TIME],x[TDTD_END_TIME],x[TDTD_START_MILEAGE],x[TDTD_END_MILEAGE],x[TDTD_IS_DRIVING],x[TDTD_MEMO],x[TDTD_IS_DEMO_CALL],x[TDTD_TEST_DRIVING_SUMMARY],x[TDTD_TEST_DRIVING_MOBILE],x[TDTD_TEST_DRIVING_NAME],x[TDTD_IS_SATISFIED],x[TDTD_LOAD_FROM])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(tdtd_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(100)	
	rows_schemardd = sqlContext.applySchema(tdtd_valid_rdd,dt_test_driving_info_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_TEST_DRIVING_INFO)
	schemardd.registerTempTable("DT_TEST_DRIVING_INFO")
	rows_schemardd.insertInto("DT_TEST_DRIVING_INFO",overwrite=True)

#生成集团参加活动行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def dossProspectOrgTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		tdpo_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_ORG).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		tdpo_temp_rdd = sc.parallelize([])
	tdpo_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_PROSPECT_ORG)
	tdpo_valid_rdd = tdpo_tf.map(lambda x: x.split("\x01")) \
							.filter(lambda x: getRightPhoneNum((x[5],x[6],x[7]))==0 ) \
							.map(lambda x:(x[TDPO_PROSPECT_ORG_ID],x[TDPO_CORP_PROSPECT_CODE],x[TDPO_CORP_NAME],x[TDPO_PHONE],x[TDPO_WORK_PHONE],x[TDPO_MOBILE],x[TDPO_GLOBAL_ID],x[TDPO_BRAND_ID],x[TDPO_DEALER_ID],x[TDPO_CUSTOMER_TYPE],x[TDPO_CUSTOMER_REQ],x[TDPO_MARKETING_EVENT_ID],x[TDPO_EMAIL],x[TDPO_MEDIA_CHANNEL],x[TDPO_INDUSTRY_TYPE],x[TDPO_DECISION_MAKER_NAME],x[TDPO_DECISION_MAKER_POSITION],x[TDPO_DECISION_MAKER_PHONE],x[TDPO_DECISION_MAKER_MOBILE],x[TDPO_CORP_TYPE],x[TDPO_DEFEAT_REASON],x[TDPO_MEMO],x[TDPO_SOURCE_ID],x[TDPO_ORIGINAL_SOURCE_TYPE],x[TDPO_HAS_OPPORTUNITY],x[TDPO_LOAD_FROM])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(tdpo_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(100)
	rows_schemardd = sqlContext.applySchema(tdpo_valid_rdd,dt_marketing_activity_org_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_ORG)
	schemardd.registerTempTable("DT_MARKETING_ACTIVITY_ORG")
	rows_schemardd.insertInto("DT_MARKETING_ACTIVITY_ORG",overwrite=True)	

#生成个人参加活动行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def dossProspectIndTable(sc,sqlContext,dt,interval=0):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		tdpi_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_IND).map(lambda x: (x[0],x[1:]))
	except:
		tdpi_temp_rdd = sc.parallelize([])
	tdpi_valid_rdd = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DOSS_TT_DOSS_PROSPECT_IND,dt,interval) \
							.filter(lambda x: getRightPhoneNum((x[4],x[5],x[6]))==0) \
							.map(lambda x:(x[TDPI_PROSPECT_IND_ID],x[TDPI_PROSPECT_CODE],x[TDPI_BIRTHDAY],x[TDPI_PROSPECT_NAME],x[TDPI_PHONE],x[TDPI_WORK_PHONE],x[TDPI_MOBILE],x[TDPI_GLOBAL_ID],x[TDPI_BRAND_ID],x[TDPI_DEALER_ID],x[TDPI_CUSTOMER_TYPE],x[TDPI_CUSTOMER_REQ],x[TDPI_MARKETING_EVENT_ID],x[TDPI_QQ_MSN],x[TDPI_EMAIL],x[TDPI_BIRTH_PLACE],x[TDPI_CAREER_TYPE],x[TDPI_CAREER_DESC],x[TDPI_POSITION_TYPE],x[TDPI_POSITION_DESC],x[TDPI_HOME_PHONE],x[TDPI_CORPORATION],x[TDPI_MEDIA_CHANNEL],x[TDPI_CONTACT_ID],x[TDPI_CERTIFICATE_TYPE],x[TDPI_CERTIFICATE_NO],x[TDPI_INDUSTRY_TYPE],x[TDPI_EDUCATION],x[TDPI_INCOME],x[TDPI_HAS_CHILD],x[TDPI_MARRIAGE_STATUS],x[TDPI_CORP_TYPE],x[TDPI_MEMO],x[TDPI_SOURCE_ID],x[TDPI_ORIGINAL_SOURCE_TYPE],x[TDPI_HAS_OPPORTUNITY],x[TDPI_LOAD_FROM],x[TDPI_DT])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str)))) \
							.union(tdpi_temp_rdd).reduceByKey(lambda x,y:DeduplicationDt(x,y)) \
							.map(lambda x: ((x[0],)+x[1])).coalesce(300)	
	rows_schemardd = sqlContext.applySchema(tdpi_valid_rdd,dt_marketing_activity_ind_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_IND)
	schemardd.registerTempTable("DT_MARKETING_ACTIVITY_IND")
	rows_schemardd.insertInto("DT_MARKETING_ACTIVITY_IND",overwrite=True)

#生成来电咨询行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def dossCallRecordTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		tddcr_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_CALL_CONSULT_INFO).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		tddcr_temp_rdd = sc.parallelize([])
	tddcr_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_DCC_CALL_RECORD)
	tddcr_valid_rdd = tddcr_tf.map(lambda x: x.split("\x01")) \
							.filter(lambda x: getRightPhoneNum((x[TDDCR_PROSPECT_MOBILE],'1'))== 0 and x[TDDCR_ENTRANCE_TIME] not in ('null','NULL','Null')) \
						  	.map(lambda x:(x[TDDCR_SHOWROOM_FLOW_ID],x[TDDCR_PROSPECT_ID],x[TDDCR_OPP_ID],x[TDDCR_ENTRANCE_TIME],x[TDDCR_DURATION],x[TDDCR_DEPARTURE_TIME],x[TDDCR_MEMO],x[TDDCR_MODEL_ID],x[TDDCR_SERIES_ID],x[TDDCR_MARKETING_EVENT_ID],x[TDDCR_PROSPECT_NAME],x[TDDCR_PROSPECT_MOBILE],x[TDDCR_BRAND_ID],x[TDDCR_DEALER_ID],x[TDDCR_PROSPECT_TYPE],x[TDDCR_CALL_COUNT_TYPE],x[TDDCR_LOAD_FROM])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(tddcr_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(100)		
	rows_schemardd = sqlContext.applySchema(tddcr_valid_rdd,dt_call_consult_info_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_CALL_CONSULT_INFO)
	schemardd.registerTempTable("DT_CALL_CONSULT_INFO")
	rows_schemardd.insertInto("DT_CALL_CONSULT_INFO",overwrite=True)	

#生成到店咨询行为信息表，电话号码有效（以1开头的11位和以01开头的12位）
def dossShowroomFlowTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		tdsf_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_SHOWROOM_CONSULT_INFO).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		tdsf_temp_rdd = sc.parallelize([])
	tdsf_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_SHOWROOM_FLOW)
	tdsf_valid_rdd = tdsf_tf.map(lambda x: x.split("\x01")) \
							.filter(lambda x: x[TDSF_FLOW_TYPE] == '1' and getRightPhoneNum((x[TDSF_CUSTOMER_MOBILE],x[TDSF_CUSTOMER_PHONE])) == 0 and x[TDSF_ENTRANCE_TIME] not in ('null','NULL','Null')) \
							.map(lambda x:(x[TDSF_SHOWROOM_FLOW_ID],x[TDSF_OPP_ID],x[TDSF_PROSPECT_ID],x[TDSF_MARKETING_EVENT_ID],x[TDSF_BRAND_ID],x[TDSF_DEALER_ID],x[TDSF_ENTRANCE_TIME],x[TDSF_CUSTOMER_COUNT],x[TDSF_SOURCE_CHANNEL],x[TDSF_SHOWROOM_ID],x[TDSF_DURATION],x[TDSF_PHONE_CONTENT],x[TDSF_GENDER],x[TDSF_VISIT_COUNT_TYPE],x[TDSF_CUSTOMER_NAME],x[TDSF_CUSTOMER_MOBILE],x[TDSF_CUSTOMER_PHONE],x[TDSF_DEPARTURE_TIME],x[TDSF_MEMO],x[TDSF_LOAD_FROM])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(tdsf_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(200)		
	rows_schemardd = sqlContext.applySchema(tdsf_valid_rdd,dt_showroom_consult_info_schema)
	schemardd = sqlContext.parquetFile(HDFS_PATH_DT_SHOWROOM_CONSULT_INFO)
	schemardd.registerTempTable("DT_SHOWROOM_CONSULT_INFO")
	rows_schemardd.insertInto("DT_SHOWROOM_CONSULT_INFO",overwrite=True)

#生成客户基本信息表（CEM），身份证号为15位或者18位，并且身份证的7-10位不为'1111'
def getCemRidtable(sc,sqlContext):
	#DT 
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		cat_temp_rdd = sqlContext.parquetFile(HDFS_PATH_CEM_CX_ASSET_TT).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		cat_temp_rdd = sc.parallelize([])
	cat_tf = sc.textFile(HDFS_PATH_CEM_CX_ASSET_TT)
	cat_valid_rdd = cat_tf.map(lambda x: x.split("\x01"))
	dt_customer_cem_rows = cat_valid_rdd.filter(lambda x:  len(x[CAT_RID]) in (15,18) and x[CAT_RID][6:10] != "1111") \
							.map(lambda x: (x[CAT_ROW_ID],x[CAT_CREATED],x[CAT_LAST_UPD],x[CAT_ACTIVITY_OPTY_COUNT],x[CAT_BIRTH_DT],x[CAT_COMPENSATION_AMOUNT],x[CAT_COMPLAINT_NUM],x[CAT_CONSUMING_AMOUNT],x[CAT_CURRENT_AGE],x[CAT_DB_LAST_UPD],x[CAT_DELIVERY_DATE],x[CAT_HISTORY_SCORE_ACCUMULATION],x[CAT_IF_ROAD_SUCCOR],x[CAT_LASEST_SERVICE_DATE],x[CAT_SCORE_BALANCE],x[CAT_SCORE_CONSUMPTION],x[CAT_SERVICE_NUM],x[CAT_ADDR_NAME],x[CAT_CITY],x[CAT_CONTACT_ASPIRATION],x[CAT_CUSTOMER_NAME],x[CAT_DB_LAST_UPD_SRC],x[CAT_EDUCATION_LEVEL],x[CAT_EMAIL_ADDR],x[CAT_FINANCIER],x[CAT_HOBBY_SUMMARY],x[CAT_INCOME_RANGE_CD],x[CAT_INDUSTRY],x[CAT_INTEREST_SUMMARY],x[CAT_JOB_NAME],x[CAT_JOB_TITLE],x[CAT_LCNS_NUM],x[CAT_MARITAL_STAT_CD],x[CAT_PAY_MOTHED],x[CAT_PRI_CONTACT_NAME],x[CAT_PRI_CONT_PHONE_NUM],x[CAT_PRI_PHONE_NUM],x[CAT_RID],x[CAT_SALES_AREA],x[CAT_SALES_TYPE],x[CAT_SEX_MF],x[CAT_STATE],x[CAT_VIN],x[CAT_ACCOUNT_TYPE],x[CAT_ENTERPRISE_ATTR],x[CAT_FAX],x[CAT_INDUSTRY_TYPE],x[CAT_OPTY_ACT_AMOUNT],x[CAT_ORG_CODE],x[CAT_ROADAID_AMOUNT],x[CAT_SGM_TOTAL_AMOUNT],x[CAT_WORK_PHONE],x[CAT_AMMOUNT_AFTERSALE],x[CAT_X_SPECIAL_FLAG],x[CAT_BUY_BEHA_TYPE],x[CAT_BUY_REASON],x[CAT_WEIBO],x[CAT_WEIXIN])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(cat_temp_rdd).partitionBy(200).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(100)		
	dt_customer_cem_rows_schemardd = sqlContext.applySchema(dt_customer_cem_rows, dt_customer_info_cem_schema)
	dt_customer_cem_schemardd = sqlContext.parquetFile(HDFS_PATH_DT_CUSTOMER_INFO_CEM)
	dt_customer_cem_schemardd.registerTempTable("dt_customer_info_cem")
	dt_customer_cem_rows_schemardd.insertInto("dt_customer_info_cem",overwrite=True)

#生成客户基本信息表（IDM），身份证号为15位或者18位，并且身份证的7-10位不为'1111'
def getIdmRidtable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		ua_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_CUSTOMER_INFO_IDM).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		ua_temp_rdd = sc.parallelize([])
	ua_tf = sc.textFile(HDFS_PATH_IDM_USER_ATTRIBUTES)
	ua_valid_rdd = ua_tf.map(lambda x: x.split("\x01")).map(lambda x: (x[0],{x[1]:x[2]}))
	ua_idm_temp_rows = ua_valid_rdd.reduceByKey(listChgRow).map(lambda x:getIdmTags(x)) \
							.filter(lambda x: x[1] != None).filter(lambda x: len(x[1]) == 15 or 18 and x[1][6:10] != '1111') \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(ua_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).repartition(100)	
	dt_customer_idm_rows_temp_schemardd = sqlContext.applySchema(ua_idm_temp_rows, dt_customer_info_idm_schema)
	dt_customer_idm_temp_schemardd = sqlContext.parquetFile(HDFS_PATH_DT_CUSTOMER_INFO_IDM)
	dt_customer_idm_temp_schemardd.registerTempTable("dt_customer_info_idm")
	dt_customer_idm_rows_temp_schemardd.insertInto("dt_customer_info_idm",overwrite=True)

# def getDossUploadTrackTable(sc,sqlContext,dt,interval=0):
# 	#DT 
# 	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
# 	try:
# 		outd_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_ORDER_UPLOAD_TRACK_DOSS).map(lambda x: (x[0],x[1:]))
# 	except:
# 		outd_temp_rdd = sc.parallelize([])
# 	dm_upload_doss_rows = getRDDfromTextFilePartitionByDT(sc,HDFS_PATH_DOSS_TT_DOSS_ORDER_UPLOAD_TRACK,dt,interval) \
# 							.filter(lambda x:  x[TDOUT_IS_VALID] == '1' and len(x[TDOUT_CERTIFICATE_NO]) in (15,18) and x[TDOUT_CERTIFICATE_NO][6:10] != '1111') \
# 							.map(lambda x: (x[TDOUT_ORDER_UPLOAD_ID],x[TDOUT_BRAND_ID],x[TDOUT_DEALER_ID],x[TDOUT_SERIES_ID],x[TDOUT_MODEL_ID],x[TDOUT_COLOR_ID],x[TDOUT_PACKAGE_ID],x[TDOUT_ASC_ID],x[TDOUT_VEHICLE_ID],x[TDOUT_VIN],x[TDOUT_ENGINE_NO],x[TDOUT_INVOICE_DATE],x[TDOUT_ORDER_NO],x[TDOUT_SND_LVL_RETAILER_NAME],x[TDOUT_INBOUND_DATE],x[TDOUT_DELIVERY_DATE],x[TDOUT_RETAIL_AMOUNT],x[TDOUT_PAYMENT_METHOD],x[TDOUT_VAT_INVOICE_NO],x[TDOUT_CAR_LICENSE_NO],x[TDOUT_DELIVERY_MILEAGE],x[TDOUT_CUSTOMER_TYPE],x[TDOUT_DELIVERY_NAME],x[TDOUT_DELIVERY_MOBILE],x[TDOUT_CONTACT_NAME],x[TDOUT_CONTACT_MOBILE],x[TDOUT_PROVINCE_ID],x[TDOUT_CITY_ID],x[TDOUT_INDUSTY_TYPE],x[TDOUT_INDUSTY_DESC],x[TDOUT_CUSTOMER_GRADE],x[TDOUT_POSTCODE],x[TDOUT_ADDRESS],x[TDOUT_VEHICLE_OWNER_NAME],x[TDOUT_CUSTOMER_RELATIONSHIP],x[TDOUT_GENDER],x[TDOUT_MARRIAGE_STATUS],x[TDOUT_CERTIFICATE_NO],x[TDOUT_BIRTH_DATE],x[TDOUT_EDUCATION],x[TDOUT_FAMILY_MONTHLY_INCOME],x[TDOUT_CAREER_TYPE],x[TDOUT_CAREER_DESC],x[TDOUT_MOBILE],x[TDOUT_PHONE],x[TDOUT_FAX],x[TDOUT_EMAIL],x[TDOUT_CONSUMER_DISTRICT_ID],x[TDOUT_CONSUMER_DISTRICT_CODE],x[TDOUT_INTERESTS],x[TDOUT_CORP_NAME],x[TDOUT_CORP_ORG_CODE],x[TDOUT_CORP_TYPE],x[TDOUT_DECISION_MAKER_NAME],x[TDOUT_DECISION_MAKER_GENDER],x[TDOUT_DECISION_MAKER_BIRTH_DATE],x[TDOUT_DECISION_MAKER_CAREER_TYPE],x[TDOUT_DECISION_MAKER_CAREER_DESC],x[TDOUT_DECISION_MAKER_MOBILE],x[TDOUT_DECISION_MAKER_CORP_PHONE],x[TDOUT_DECISION_MAKER_FAX],x[TDOUT_DECISION_MAKER_EMAIL],x[TDOUT_REPORTING_DATE],x[TDOUT_UPLOAD_CATEGORY],x[TDOUT_STATUS],x[TDOUT_INVOICE_NO],x[TDOUT_ORDER_ID],x[TDOUT_CUSTOMER_CODE],x[TDOUT_POSITION_TYPE],x[TDOUT_POSITION_DESC],x[TDOUT_WITHDRAW_DATE],x[TDOUT_INTEREST_CATEGORIES],x[TDOUT_EFF_DATE],x[TDOUT_END_DATE])) \
# 							.map(lambda x: (x[0],(x[1:] +(today_str,today_str)))) \
# 							.union(outd_temp_rdd).reduceByKey(lambda x,y:DeduplicationDt(x,y)) \
# 							.map(lambda x: ((x[0],)+x[1])).repartition(300)		
# 	dm_upload_doss_rows_schemardd = sqlContext.applySchema(dm_upload_doss_rows, dm_order_upload_track_doss_schema)
# 	dm_upload_track_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_ORDER_UPLOAD_TRACK_DOSS)
# 	dm_upload_track_schemardd.registerTempTable("dm_order_upload_track_doss")
# 	dm_upload_doss_rows_schemardd.insertInto("dm_order_upload_track_doss",overwrite=True)	

if __name__ == '__main__':
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getCemRidtable(sc,sqlContext)
	getIdmRidtable(sc,sqlContext)		
	cacCallTable(sc,sqlContext)
	dossTestDrivingTable(sc,sqlContext)
	dossProspectOrgTable(sc,sqlContext)
	dossProspectIndTable(sc,sqlContext,dt,interval)
	dossCallRecordTable(sc,sqlContext)
	dossShowroomFlowTable(sc,sqlContext)
	# getDossUploadTrackTable(sc,sqlContext,dt,interval)
