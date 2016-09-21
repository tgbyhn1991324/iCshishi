# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_cemTagCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.config.table_path import HDFS_PATH_DT_CUSTOMER_INFO_CEM, HDFS_PATH_RID_TAG_BASE_FROM_CEM_CX_ASSET_TT
from mathartsys.supcap.tagFromMart.peopleValueTag.peopleValueFunction import getRIDTags
from mathartsys.supcap.sys_common.tag_schema import rid_tag_base_cem_cx_asset_tt_schema,formatRowFromSchema

#取最新时间的记录为有效
def getLatestTags(x,y):
	dt1 = datetime.datetime.strptime(x[1],"%Y-%m-%d %H:%M:%S.0")
	dt2 = datetime.datetime.strptime(y[1],"%Y-%m-%d %H:%M:%S.0")
	if dt1 < dt2:
		tag = y[0]
		dt = dt2.strftime("%Y-%m-%d %H:%M:%S.0")
	else:
		tag = x[0]
		dt = dt1.strftime("%Y-%m-%d %H:%M:%S.0")
	return (tag,dt)

def addRid(row):
	dic = row[1][0]
	dic['tag_id'] = row[0]
	return dic

#生成车主属性标签表
def getCemRidtags(sc,sqlContext):
	dcic_pf = sqlContext.parquetFile(HDFS_PATH_DT_CUSTOMER_INFO_CEM)
	rid_tags_rows = dcic_pf.map(lambda x: getRIDTags(x)) \
	                             .reduceByKey(getLatestTags).map(lambda x: addRid(x)) \
	                             .map(lambda x: formatRowFromSchema(x, rid_tag_base_cem_cx_asset_tt_schema))
	rid_tags_rows_schemardd = sqlContext.applySchema(rid_tags_rows, rid_tag_base_cem_cx_asset_tt_schema	)
	rid_tags_schemardd = sqlContext.parquetFile(HDFS_PATH_RID_TAG_BASE_FROM_CEM_CX_ASSET_TT)
	rid_tags_schemardd.registerTempTable("rid_tag_from_cem")
	rid_tags_rows_schemardd.insertInto("rid_tag_from_cem",overwrite=True)

if __name__ == '__main__':
	getCemRidtags(sc,sqlContext)
