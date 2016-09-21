# -*- coding: utf-8 -*-
import sys

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_cacTagCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.config.table_path import HDFS_PATH_DT_CALL_COMM_INFO, HDFS_PATH_PHONE_TAG_BASE_FROM_CAC_S_SRV_REQ
from mathartsys.supcap.tagFromMart.phoneTag.tag_from_s_srv_req import getTagsFromS_srv_req
from mathartsys.supcap.sys_common.tag_schema import *

def getRightPhoneNum(phone):
	# phone_row = ['130','131','132','133','134','135','136','137','138','139','150','151','152','153','155','156','157','158','159','176','177','178','180','181','182','183','184','185','186','187','188','189']
	phone_tag = 1
	if len(phone) == 11:
		if phone[0] == '1':
			phone_tag = 0
		else:
			phone_tag = 1
	elif len(phone) == 12:
		if phone[0:2] == '01':
			phone_tag = 0
		else:
			phone_tag = 1
	else:
		phone_tag = 1
	return phone_tag

def addPhone(row):
	dic = row[1]
	if len(row[0]) == 11: 
		dic['tag_id'] = row[0]
	elif len(row[0]) ==12:
		dic['tag_id'] = row[0][1:]
	return dic

#生成电话沟通行为标签表
def getCacPhonetags(sc,sqlContext):
	daci_pf = sqlContext.parquetFile(HDFS_PATH_DT_CALL_COMM_INFO)
	phone_tags_rows = daci_pf.map(lambda x: (x.alt_con_ph_num,(x.sr_area,x.sr_sub_area,x.created))).groupByKey() \
									.map(lambda x: (x[0],getTagsFromS_srv_req(x[1]))) \
									.map(lambda x: addPhone(x)) \
									.map(lambda x: formatRowFromSchema(x, phone_tag_base_cac_s_srv_req_schema))
	phone_tags_rows_schemardd = sqlContext.applySchema(phone_tags_rows, phone_tag_base_cac_s_srv_req_schema)
	phone_tags_schemardd = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG_BASE_FROM_CAC_S_SRV_REQ)
	phone_tags_schemardd.registerTempTable("phone_tag_from_cac")
	phone_tags_rows_schemardd.insertInto("phone_tag_from_cac",overwrite=True)

if __name__ == '__main__':
	getCacPhonetags(sc,sqlContext)