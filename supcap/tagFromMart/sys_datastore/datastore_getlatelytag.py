# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_LATELY_TAG")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tablepath import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_schema import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_function import getLate,getlatetag

def getLatelytags(sc,sqlContext):
	repair_order_latelyTime = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL).map(lambda x: (x.vin,x.start_time)) \
						.reduceByKey(getLate).map(lambda x: getlatetag(x))
	repair_order_latelyTimeSchemaRdd = sqlContext.applySchema(repair_order_latelyTime,datastore_tag_lately_schema).coalesce(200)
	repair_order_lately_tag = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_LATELY)
	repair_order_lately_tag.registerTempTable("repair_order_lately")				  
	repair_order_latelyTimeSchemaRdd.insertInto("repair_order_lately",overwrite=True)

if __name__ == '__main__':
	getLatelytags(sc,sqlContext)