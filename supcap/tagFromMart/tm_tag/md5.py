# -*- coding: utf-8 -*-
import sys

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_Encryption")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)
	try:
		from pyspark.sql.types import *
	except:
		from pyspark.sql import *

from mathartsys.supcap.tagFromMart.config.table_path import  HDFS_PATH_RID_TAG_BASE_FROM_CEM_CX_ASSET_TT 
from mathartsys.supcap.tagFromMart.tm_tag.vin.tm_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

HDFS_PATH_RID_MD5 = "/project/supcap/iinsight/rid_md5"	
HDFS_PATH_VIN_MD5 = "/project/supcap/iinsight/vin_md5"											


def md5(str):
	import hashlib
	import types
	try:
		m = hashlib.md5()   
		m.update(str)
		return m.hexdigest()
	except Exception, e:
		return None

rid_tag_md5_schema = StructType([
					StructField("tag_id",                  StringType(), True),
					StructField("md5_value",                  StringType(), True)
					])

vin_tag_md5_schema = StructType([
					StructField("tag_id",                  StringType(), True),
					StructField("md5_value",                  StringType(), True)
					])

try:
	initParquetFile(rid_tag_md5_schema, HDFS_PATH_RID_MD5)
except Exception, e:
	print "initParquetFile fail1" 

try:
	initParquetFile(vin_tag_md5_schema, HDFS_PATH_VIN_MD5)
except Exception, e:
	print "initParquetFile fail2"



def getRIDmd5(sc,sqlContext):
	rid_cat_tf = sqlContext.parquetFile(HDFS_PATH_RID_TAG_BASE_FROM_CEM_CX_ASSET_TT)
	rid_md5_rows = rid_cat_tf.map(lambda x: (x[0],md5(x[0])))
	rid_md5_rows_schemardd = sqlContext.applySchema(rid_md5_rows,rid_tag_md5_schema)
	rid_md5_schemardd= sqlContext.parquetFile(HDFS_PATH_RID_MD5)
	rid_md5_schemardd.registerTempTable("rid_md5")
	rid_md5_rows_schemardd.insertInto("rid_md5",overwrite=True)

def getVINmd5(sc,sqlContext):
	vin_cat_tf = sqlContext.parquetFile(HDFS_PATH_VIN_TAG)
	vin_md5_rows = vin_cat_tf.map(lambda x: (x[0],md5(x[0])))
	vin_md5_rows_schemardd = sqlContext.applySchema(vin_md5_rows,vin_tag_md5_schema)
	vin_md5_schemardd= sqlContext.parquetFile(HDFS_PATH_VIN_MD5)
	vin_md5_schemardd.registerTempTable("vin_md5")
	vin_md5_rows_schemardd.insertInto("vin_md5",overwrite=True)

if __name__ == '__main__':
	getRIDmd5(sc,sqlContext)
	getVINmd5(sc,sqlContext)

