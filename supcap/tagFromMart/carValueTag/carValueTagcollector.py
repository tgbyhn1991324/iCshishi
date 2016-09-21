# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_vehicleTableCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.config.table_path import *
from mathartsys.supcap.tagFromMart.config.table_schema import *
from mathartsys.supcap.tagFromMart.carValueTag.carValueFunction import *

#生成车辆属性标签表
def getCarVinTag(sc,sqlContext):
	#读取最新的车型对应关系map
	series_map = sqlContext.parquetFile('/project/supcap/iinsight/data_mart/lookup_table/tag_value') \
				.map(lambda x: x.tag).reduce(lambda x,y:dict(x,**y))
	vin_pf = sqlContext.parquetFile(HDFS_PATH_DT_VEHICLE_INFO_TOTAL)
	vin_tags_rows = vin_pf.map(lambda x: getCarTagCollector(x,series_map)) \
						.map(lambda x: formatRowFromSchema(x, vin_tag_base_car_schema)) 
	vin_tags_rows_schemardd = sqlContext.applySchema(vin_tags_rows, vin_tag_base_car_schema)
	vin_tags_schemardd = sqlContext.parquetFile(HDFS_PATH_VIN_TAG_CAR)
	vin_tags_schemardd.registerTempTable("vin_tag_car")
	vin_tags_rows_schemardd.insertInto("vin_tag_car",overwrite=True)

if __name__ == '__main__':
	getCarVinTag(sc,sqlContext)