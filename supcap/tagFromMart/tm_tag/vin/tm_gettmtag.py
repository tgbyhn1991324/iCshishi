# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_TM_TAG")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tm_tag.vin.tm_tablepath import *
from mathartsys.supcap.tagFromMart.tm_tag.vin.tm_schema import *

def unionDict(dict1,dict2):
	if dict1 and dict2:
		return dict(dict1, **dict2)
	elif not dict1:
		return dict2
	elif not dict2:
		return dict1

def delnulldate(dict1):
	dict2 = {}
	for key in dict1.keys():
		if isinstance(dict1[key],unicode):
			dict2 = dict(dict2, **{key:dict1[key]})
			del dict1[key]
	return dict1,dict2

def formatRowFromSchema(dictRow, schema):
	ret = []
	for field in schema.fields:
		if dictRow.has_key(field.name):
			if isinstance(dictRow[field.name],int) or isinstance(dictRow[field.name],long):
				ret.append(int(dictRow[field.name]))
			else:
				ret.append(dictRow[field.name])
		else:
			ret.append(None)
	return ret

def gettmtag(sc,sqlContext):
	vin_repair_lm3 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM3).map(lambda x: (x.tag_id,x.asDict()))
	vin_repair_lm6 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM6).map(lambda x: (x.tag_id,x.asDict()))
	vin_repair_lm12 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM12).map(lambda x: (x.tag_id,x.asDict()))
	vin_repair_1yb = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_TAG_1YB).map(lambda x: (x.tag_id,x.asDict()))
	vin_repair_lately = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_LATELY).map(lambda x: (x.tag_id,x.asDict()))
	vin_parts_lm3 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM3).map(lambda x: (x.tag_id,x.asDict()))
	vin_parts_lm6 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM6).map(lambda x: (x.tag_id,x.asDict()))
	vin_parts_lm12 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM12).map(lambda x: (x.tag_id,x.asDict()))
	vin_claim = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_CLAIM_TAG).map(lambda x: (x.tag_id,x.asDict()))
	vin_car =sqlContext.parquetFile(HDFS_PATH_VIN_CAR_TAG).map(lambda x: (x.tag_id,x.asDict()))
	vintagrdd = vin_repair_lm3.union(vin_repair_lm6).union(vin_repair_lm12).union(vin_repair_1yb).union(vin_repair_lately).union(vin_parts_lm3).union(vin_parts_lm6).union(vin_parts_lm12).union(vin_claim).union(vin_car) \
						.reduceByKey(unionDict).map(lambda x: formatRowFromSchema(x[1],vin_tag_schema)).repartition(100)
	vintagtable_schemardd = sqlContext.applySchema(vintagrdd,vin_tag_schema)
	vintagtable = sqlContext.parquetFile(HDFS_PATH_VIN_TAG)
	vintagtable.registerTempTable("vintagtable")				  
	vintagtable_schemardd.insertInto("vintagtable",overwrite=True)

if __name__ == '__main__':
	gettmtag(sc,sqlContext)