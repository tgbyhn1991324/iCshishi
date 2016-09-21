import sys 
import time 
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_TableInit")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.config.table_path import *
from mathartsys.supcap.tagFromMart.config.table_schema import *

def initParquetFile(schema,path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(rid_tag_base_cem_cx_asset_tt_schema, HDFS_PATH_RID_TAG_BASE_FROM_CEM_CX_ASSET_TT)
	except Exception, e:
		print "initParquetFile fail1" +str(e)
	try:
		initParquetFile(phone_tag_base_cac_s_srv_req_schema, HDFS_PATH_PHONE_TAG_BASE_FROM_CAC_S_SRV_REQ)
	except Exception, e:
		print "initParquetFile fail2" +str(e)
	try:
		initParquetFile(phone_tag_base_doss_schema,HDFS_PATH_PHONE_TAG_BASE_FROM_DOSS)
	except Exception, e:
		print "initParquetFile fail4" +str(e)
	try:
		initParquetFile(vin_tag_base_car_schema, HDFS_PATH_VIN_TAG_CAR)
	except Exception, e:
		print "initParquetFile fail5" +str(e)
	#miaozhen
	try:
		initParquetFile(cookie_tag_base_dmp_tag_schema, HDFS_PATH_COOKIE_TAG_BASE_FROM_MIAOZHEN_DMP_TAG)
	except Exception, e:
		print "initParquetFile fail6" +str(e)