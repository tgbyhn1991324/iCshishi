# -*- coding: utf-8 -*-
import sys
from operator import add

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_dossPhoneTagCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.config.table_path import *        
from mathartsys.supcap.tagFromMart.config.table_schema import *
from mathartsys.supcap.superid.doss_phone_tags import *
from mathartsys.supcap.tagFromMart.phoneTag.phoneTagFunction import *

#生成线下行为标签表
def getPhoneTagFromDoss(sc,sqlContext):
	#tt_doss_showroom_flow_tagCollector 来店咨询状况
	dci_pf = sqlContext.parquetFile(HDFS_PATH_DT_SHOWROOM_CONSULT_INFO)
	# (u'13868201141', (u'2010-12-31 00:00:00.0', u'null'))
	phone_showroom_rows = dci_pf.map(lambda x: ((x.entrance_time,x.duration),getPhoneList((x.customer_mobile,x.customer_phone)))) \
								.filter(lambda x: x[1] != []).flatMapValues(lambda x: x).map(lambda x: (x[1],getPhoneTags(x[0]))) \
								.reduceByKey(PhoneTagCount).map(lambda x: (x[0],getPhoneDict(x[1],1)))
	#tt_doss_dcc_call_record 电话咨询状况
	cci_pf = sqlContext.parquetFile(HDFS_PATH_DT_CALL_CONSULT_INFO)
	phone_call_rows = cci_pf.map(lambda x: (x.prospect_mobile,(x.entrance_time,x.duration))).map(lambda x: (x[0],getPhoneTags(x[1]))) \
							.reduceByKey(PhoneTagCount).map(lambda x: (x[0],getPhoneDict(x[1],2)))

	#tt_doss_prospect_ind_tagCollector 存储有不同日期 获取参加市场活动次数(个人)
	mai_pf = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_IND)
	# 字段编号 PHONE=4 WORK_PHONE=5 MOBILE=6
	phone_marketingInd_rows = mai_pf.map(lambda x: (1,getPhoneList([x.phone,x.work_phone,x.mobile]))) \
								.flatMapValues(lambda x: x).map(lambda x: (x[1],x[0])).reduceByKey(add)
	#tt_doss_prospect_org_tagCollector  获取参加市场活动次数(集体)
	mao_pf = sqlContext.parquetFile(HDFS_PATH_DT_MARKETING_ACTIVITY_ORG)
	phone_marketingOrg_rows = mao_pf.map(lambda x: (1,getPhoneList([x.phone,x.work_phone,x.mobile]))) \
								.flatMapValues(lambda x: x).map(lambda x: (x[1],x[0])).reduceByKey(add)
	phone_marketing_union = phone_marketingInd_rows.union(phone_marketingOrg_rows).reduceByKey(add).map(lambda x: (x[0],getPhoneDict(x[1],3)))
				
	#tt_doss_test_driving 获取试驾状况
	tdi_pf = sqlContext.parquetFile(HDFS_PATH_DT_TEST_DRIVING_INFO)
	phone_test_driving_rows = tdi_pf.map(lambda x: (x.test_driving_mobile,(x.test_driving_date,(int(x.end_mileage)-int(x.start_mileage))))) \
						.map(lambda x: (x[0],getPhoneTags(x[1]))).reduceByKey(PhoneTagCount).map(lambda x: (x[0],getPhoneDict(x[1],4)))

	phone_tags_rows = phone_showroom_rows.union(phone_call_rows).union(phone_marketing_union) \
						.union(phone_test_driving_rows) \
						.reduceByKey(combineDict).map(lambda x: (dict({"tag_id":x[0]},**x[1]))) \
						.map(lambda x: formatRowFromSchema(x, phone_tag_base_doss_schema)).coalesce(100)
	phone_tags_rows_schemardd = sqlContext.applySchema(phone_tags_rows, phone_tag_base_doss_schema)
	phone_tags_schemardd = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG_BASE_FROM_DOSS)
	phone_tags_schemardd.registerTempTable("phone_tag_from_doss")
	phone_tags_rows_schemardd.insertInto("phone_tag_from_doss",overwrite=True)

if __name__ == '__main__':
	getPhoneTagFromDoss(sc,sqlContext)
