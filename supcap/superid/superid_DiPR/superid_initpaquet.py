# -*- coding: utf-8 -*-
import sys 
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_TableInit")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.superidDiPR.table_path import *
from mathartsys.supcap.superid.superidDiPR.table_schema import *      

def initParquetFile(schema,path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(diphoneweibo_rdd_schema, HDFS_PATH_DI_PHONE_WEIBO)
	except Exception, e:
		print "initParquetFile DI_PHONE_WEIBO fail" +str(e)
	try:
		initParquetFile(diridweibo_rdd_schema, HDFS_PATH_DI_RID_WEIBO)
	except Exception, e:
		print "initParquetFile DI_RID_WEIBO fail" +str(e)
	try:
		initParquetFile(divinweibo_rdd_schema, HDFS_PATH_DI_VIN_WEIBO)
	except Exception, e:
		print "initParquetFile DI_VIN_WEIBO fail" +str(e)
	try:
		initParquetFile(diphoneemail_rdd_schema, HDFS_PATH_DI_PHONE_EMAIL)
	except Exception, e:
		print "initParquetFile DI_PHONE_EMAIL fail" +str(e)
	try:
		initParquetFile(divinemail_rdd_schema, HDFS_PATH_DI_VIN_EMAIL)
	except Exception, e:
		print "initParquetFile DI_VIN_EMAIL fail" +str(e)
	try:
		initParquetFile(diridemail_rdd_schema, HDFS_PATH_DI_RID_EMAIL)
	except Exception, e:
		print "initParquetFile DI_RID_EMAIL fail" +str(e)
	try:
		initParquetFile(dilicenseemail_rdd_schema, HDFS_PATH_DI_LICENSE_EMAIL)
	except Exception, e:
		print "initParquetFile DI_LICENSE_EMAIL fail" +str(e)
	try:
		initParquetFile(diridweixin_rdd_schema, HDFS_PATH_DI_RID_WEIXIN)
	except Exception, e:
		print "initParquetFile DI_RID_WEIXIN fail" +str(e)
	try:
		initParquetFile(dilicenseweixin_rdd_schema, HDFS_PATH_DI_LICENSE_WEIXIN)
	except Exception, e:
		print "initParquetFile DI_LICENSE_WEIXIN fail" +str(e)
	try:
		initParquetFile(divinweixin_rdd_schema, HDFS_PATH_DI_VIN_WEIXIN)
	except Exception, e:
		print "initParquetFile DI_VIN_WEIXIN fail" +str(e)
	try:
		initParquetFile(diphoneweixin_rdd_schema, HDFS_PATH_DI_PHONE_WEIXIN)
	except Exception, e:
		print "initParquetFile DI_PHONE_WEIXIN fail" +str(e)
	try:
		initParquetFile(diphoneweixin_rdd_schema, HDFS_PATH_DI_PHONE_WEIXIN)
	except Exception, e:
		print "initParquetFile DI_PHONE_WEIXIN fail" +str(e)
	try:
		initParquetFile(divinrid_rdd_schema, HDFS_PATH_DI_RID_VIN)
	except Exception, e:
		print "initParquetFile DI_VIN_RID fail" +str(e)
	try:
		initParquetFile(diphonerid_rdd_schema, HDFS_PATH_DI_RID_PHONE)
	except Exception, e:
		print "initParquetFile DI_PHONE_RID fail" +str(e)
	try:
		initParquetFile(dilicenserid_rdd_schema, HDFS_PATH_DI_RID_LICENSE)
	except Exception, e:
		print "initParquetFile DI_LICENSE_RID fail" +str(e)
	try:
		initParquetFile(divinphone_rdd_schema, HDFS_PATH_DI_VIN_PHONE)
	except Exception, e:
		print "initParquetFile DI_VIN_PHONE fail" +str(e)
	try:
		initParquetFile(divinlicense_rdd_schema, HDFS_PATH_DI_VIN_LICENSE)
	except Exception, e:
		print "initParquetFile DI_VIN_LICENSE fail" +str(e)
	try:
		initParquetFile(diphonelicense_rdd_schema, HDFS_PATH_DI_PHONE_LICENSE)
	except Exception, e:
		print "initParquetFile DI_VIN_LICENSE fail" +str(e)

