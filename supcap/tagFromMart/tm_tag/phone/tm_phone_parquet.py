# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *

conf = SparkConf().setAppName("PySpark_SUPCAP_TM_phone_tag_init")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tm_tag.phone.tm_phone_schema import *
from mathartsys.supcap.tagFromMart.tm_tag.phone.tm_phone_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(phone_tag_schema, HDFS_PATH_PHONE_TAG)
	except Exception, e:
		print "initParquetFile fail1", str(e)