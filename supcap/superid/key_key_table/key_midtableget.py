# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_KEY_KEY_MIDTABLE")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.key_key_table.key_tablepath import *
from mathartsys.supcap.superid.key_key_table.key_function import *
from mathartsys.supcap.superid.key_key_table.key_schema import *
from mathartsys.supcap.superid.key_key_table.user_attributes_table import *
from mathartsys.supcap.superid.key_key_table.tt_doss_repair_order_table import *
from mathartsys.supcap.superid.key_key_table.tt_doss_order_upload_track_table import *

# 处理非法json格式数据
def getjson(x):
	import json
	try:
		return json.loads(x[2])
	except:
		return {}

# 生成IDM中间表
def getIDMmid(sc,sqlContext):
	idm_rdd = sc.textFile(HDFS_PATH_USER_ATTRIBUTES).map(lambda x: x.split("\x01")) \
						.filter(lambda x: x[IDM_ATTRIBUTE_ID] in ('1395304','5934148','5934153')) \
						.map(lambda x: (x[IDM_USER_ID],x[IDM_ATTRIBUTE_ID],x[IDM_ATTRIBUTE_VALUE]))
	idm_rdd.persist()
	idm_rdd_rid = idm_rdd.filter(lambda x: x[1] == '5934148' and len(x[2]) in (18,15)).map(lambda x: (x[0],x[2]))
	idm_rdd_vin1 = idm_rdd.filter(lambda x: x[1] == '1395304' and len(x[2]) == 17).map(lambda x: (x[0],x[2]))
	idm_rdd_json = idm_rdd.filter(lambda x: x[1] == '5934153' ).map(lambda x: (x[0],getjson(x))).filter(lambda x: x[1])
	idm_rdd_json.persist()
	idm_rdd_auto = idm_rdd_json.map(lambda x: (x[0],x[1][0].get('AUTO_VH_INFO'))).filter(lambda x: x[1])
	idm_rdd_contact = idm_rdd_json.map(lambda x: (x[0],x[1][0].get('CONTACT_INFO'))).filter(lambda x: x[1])
	idm_rdd_linsence = idm_rdd_auto.map(lambda x: (x[0],x[1][0].get('CAR_LINSENCE'))).filter(lambda x: x[0] and x[1])
	idm_rdd_vin2 = idm_rdd_auto.map(lambda x: (x[0],x[1][0].get('VIN'))).filter(lambda x: x[0] and x[1])
	idm_rdd_phone = idm_rdd_contact.map(lambda x: (x[0],x[1][0].get('CELL_PH_NUM'))).filter(lambda x: x[0] and x[1])
	idm_rdd_email = idm_rdd_contact.map(lambda x: (x[0],x[1][0].get('EMAIL_ADDR'))).filter(lambda x: x[0] and x[1])
	idm_rdd_weixin = idm_rdd_contact.map(lambda x: (x[0],x[1][0].get('WEIXIN_NUM'))).filter(lambda x: x[0] and x[1])
	union_rdd = idm_rdd_vin1.fullOuterJoin(idm_rdd_rid).fullOuterJoin(idm_rdd_phone) \
					.map(lambda x: (x[0],(x[1][0] or ('null','null')) + ((x[1][1],) or ('null',)))) \
					.fullOuterJoin(idm_rdd_email) \
					.map(lambda x: (x[0],(x[1][0] or ('null','null','null')) + ((x[1][1],) or ('null',)))) \
					.fullOuterJoin(idm_rdd_linsence) \
					.map((lambda x: (x[0],(x[1][0] or ('null','null','null','null')) + ((x[1][1],) or ('null',))))) \
					.fullOuterJoin(idm_rdd_weixin) \
					.map((lambda x: (x[0],(x[1][0] or ('null','null','null','null','null')) + ((x[1][1],) or ('null',))))) \
					.fullOuterJoin(idm_rdd_vin2) \
					.map(lambda x: (x[1][0] or ('null','null','null','null','null')) + ((x[1][1],) or ('null',))) \
					.map(lambda x: (x[1:-1] + (x[0],x[-1])))
	idm_temptable_schemardd = sqlContext.applySchema(union_rdd,idm_temptable_schema).coalesce(20)
	idm_temptable = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE)
	idm_temptable.registerTempTable("idm_temptable")				  
	idm_temptable_schemardd.insertInto("idm_temptable",overwrite=True)

# 生成repairoder中间表
def getRepairOrdermid(sc,sqlContext,dt,interval=0):
	try:	
		repair_temp_rdd = sqlContext.parquetFile(HDFS_PATH_REPAIR_ORDER_TEMP_TABLE).map(lambda x:((x[0:2],x[:])))
	except:
		repair_temp_rdd = sc.parallelize([])
	tt_asc_repair_order_rdd = getTextFileBydt(sc,HDFS_PATH_TT_ASC_REPAIR_ORDER,dt,interval) \
							  .filter(lambda x: (x[R_DELIVERER_PHONE] != 'null' or x[R_DELIVERER_MOBILE] != 'null' or x[R_LICENSE] != 'null')) \
							  .map(lambda x: ((x[R_ASC_CODE],x[R_RO_NO]),(x[R_ASC_CODE],x[R_RO_NO],x[R_VIN],x[R_LICENSE],x[R_DELIVERER_PHONE],x[R_DELIVERER_MOBILE],x[R_START_TIME],x[R_DT]))) \
							  .union(repair_temp_rdd) \
							  .filter(lambda x: x[1][-2] != 'null') \
							  .reduceByKey(Deduplication).map(lambda x:	 x[1][:-1] + (long(x[1][-1]),))
	tt_asc_repair_order_schemardd = sqlContext.applySchema(tt_asc_repair_order_rdd,repair_order_temptable_schema).repartition(800)
	tt_asc_repair_order = sqlContext.parquetFile(HDFS_PATH_REPAIR_ORDER_TEMP_TABLE)
	tt_asc_repair_order.registerTempTable("tt_asc_repair_order")				  
	tt_asc_repair_order_schemardd.insertInto("tt_asc_repair_order",overwrite=True)	

# 生成dossupload中间表
def gettt_doss_order_upload_track_mid(sc,sqlContext,dt,interval=0):
	today = datetime.datetime.today()
	interval_new = (today - datetime.datetime(2015,4,30)).days
	tt_doss_order_upload_track_rdd = getTextFileBydt(sc,HDFS_PATH_TT_DOSS_ORDER_UPLOAD_TRACK,'20150430',interval_new) \
							  .map(lambda x: (x[DOSS_UT_ORDER_UPLOAD_ID],(x[DOSS_UT_VIN],x[DOSS_UT_CERTIFICATE_NO],x[DOSS_UT_MOBILE],x[DOSS_UT_DELIVERY_MOBILE],x[DOSS_UT_CONTACT_MOBILE],x[DOSS_UT_PHONE],x[DOSS_UT_EMAIL],x[DOSS_UT_LAST_LOAD_DATE],x[DOSS_UT_WITHDRAW_DATE],x[DOSS_UT_IS_VALID],x[DOSS_UT_DT]))) \
							  .reduceByKey(Deduplication) \
							  .filter(lambda x: (not x[1][-3] or x[1][-3] in ('null','NULL','Null')) and x[1][-2] == '1') \
							  .map(lambda x: x[1][:-3])
	tt_doss_order_upload_track_schemardd = sqlContext.applySchema(tt_doss_order_upload_track_rdd,upload_track_temptable_schema).repartition(50)
	tt_doss_order_upload_track = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE)
	tt_doss_order_upload_track.registerTempTable("tt_doss_order_upload_track")				  
	tt_doss_order_upload_track_schemardd.insertInto("tt_doss_order_upload_track",overwrite=True)

if __name__ == '__main__':
	getIDMmid(sc,sqlContext)
	try:
		dt = sys.argv[1]
		interval = int(sys.argv[2])
	except Exception, e:
		today = datetime.datetime.today()
		dt = (today - datetime.timedelta(5)).strftime("%Y%m%d")
		interval = 5
	getRepairOrdermid(sc,sqlContext,dt,interval)
	gettt_doss_order_upload_track_mid(sc,sqlContext,dt,interval)
