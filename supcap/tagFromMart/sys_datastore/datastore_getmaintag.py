# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_IDASHBOARD_init")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tablepath import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_function import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_union_table import *
# from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tt_doss_vehicle_table import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_schema import *

def getDatastoreMainTag(sc,sqlContext):
	today = datetime.datetime.today()
	repairBalance_rdd = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL)
	unioned_rdd = repairBalance_rdd.map(lambda x: (x.vin,(x.brand_name,x.buy_date,x.asc_code,x.start_time,x.balance_time,x.in_mileage,x.out_mileage,x.total_amount,x.repair_type_desc))) \
						.filter(lambda x: x[1][0] != None)
	unioned_rdd.persist()
	#12月之前维修标签
	datastore_tag_1yb = unioned_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][3],"%Y-%m-%d %H:%M:%S.0")).days>365) \
						 .groupByKey().map(lambda x: (x[0],getTagsFromDatastore(x[1]))).map(lambda x: ((x[0],)+tuple(x[1])))
	datastore_tag_1yb_schemardd = sqlContext.applySchema(datastore_tag_1yb,datastore_tag_1YB_schema).repartition(50)
	tag_1yb = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_1YB)
	tag_1yb.registerTempTable("tag_1yb")				  
	datastore_tag_1yb_schemardd.insertInto("tag_1yb",overwrite=True)
	#12个月维修标签
	datastore_tag_lm12 = unioned_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][3],"%Y-%m-%d %H:%M:%S.0")).days<=365) \
						 .groupByKey().map(lambda x: (x[0],getTagsFromDatastore(x[1]))).map(lambda x: ((x[0],)+tuple(x[1])))
	datastore_tag_lm12_schemardd = sqlContext.applySchema(datastore_tag_lm12,datastore_tag_LM12_schema).repartition(50)
	tag_lm12 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM12)
	tag_lm12.registerTempTable("tag_lm12")				  
	datastore_tag_lm12_schemardd.insertInto("tag_lm12",overwrite=True)
	#6个月维修标签
	datastore_tag_lm6 = unioned_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][3],"%Y-%m-%d %H:%M:%S.0")).days<=180) \
						.groupByKey().map(lambda x: (x[0],getTagsFromDatastore(x[1]))).map(lambda x: ((x[0],)+tuple(x[1])))
	datastore_tag_lm6_schemardd = sqlContext.applySchema(datastore_tag_lm6,datastore_tag_LM6_schema).repartition(50)
	tag_lm6 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM6)
	tag_lm6.registerTempTable("tag_lm6")				  
	datastore_tag_lm6_schemardd.insertInto("tag_lm6",overwrite=True)
	#3个月维修标签
	datastore_tag_lm3 = unioned_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][3],"%Y-%m-%d %H:%M:%S.0")).days<=90) \
						.groupByKey().map(lambda x: (x[0],getTagsFromDatastore(x[1]))).map(lambda x: ((x[0],)+tuple(x[1])))
	datastore_tag_lm3_schemardd = sqlContext.applySchema(datastore_tag_lm3,datastore_tag_LM3_schema).repartition(50)
	tag_lm3 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM3)
	tag_lm3.registerTempTable("tag_lm3")				  
	datastore_tag_lm3_schemardd.insertInto("tag_lm3",overwrite=True)

if __name__ == '__main__':
	getDatastoreMainTag(sc,sqlContext)