# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_TM_PHONE_TAG")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tm_tag.phone.tm_phone_tablepath import *
from mathartsys.supcap.tagFromMart.tm_tag.phone.tm_phone_schema import *

def combineDict(dict1,dict2):
	return (dict(dict1,**dict2))

def gettmPhonetag(sc,sqlContext):
	phone_cac = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG_BASE_FROM_CAC_S_SRV_REQ).map(lambda x: (x[0],x.asDict()))
	phone_doss = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG_BASE_FROM_DOSS).map(lambda x: (x[0],x.asDict()))
	phonetagrdd = phone_cac.union(phone_doss).reduceByKey(combineDict).map(lambda x: (dict({"tag_id":x[0]},**x[1]))) \
						.map(lambda x: formatRowFromSchema(x,phone_tag_schema)).coalesce(100)
	phonetagtable_schemardd = sqlContext.applySchema(phonetagrdd,phone_tag_schema)
	phonetagtable = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG)
	phonetagtable.registerTempTable("phonetagtable")				  
	phonetagtable_schemardd.insertInto("phonetagtable",overwrite=True)

if __name__ == '__main__':
	gettmPhonetag(sc,sqlContext)