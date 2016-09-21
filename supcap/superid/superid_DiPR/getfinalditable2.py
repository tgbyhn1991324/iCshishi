# -*- coding: utf-8 -*-
from __future__ import division
import sys
import datetime
import math

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_DI2")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.superid_DiPR.dischema import *
from mathartsys.supcap.superid.superid_DiPR.dipath import *
from mathartsys.supcap.superid.superid_DiPR.superid_map import *
from mathartsys.supcap.superid.superid_DiPR.superid_function import *
from mathartsys.supcap.superid.key_key_table.key_tablepath import *

def diphone_licenseGet(sc,sqlContext):
	phonelicense = getfixDi(sqlContext,HDFS_PATH_TT_PHONE_LICENSE,PL_SYSMAP,PL_CMAP).coalesce(500)
	rdd = getDiMulToDiMul(phonelicense).coalesce(600)
	diphonelicense_schemardd = sqlContext.applySchema(rdd,diphonelicense_rdd_schema)
	diphonelicense = sqlContext.parquetFile(HDFS_PATH_DI_PHONE_LICENSE)
	diphonelicense.registerTempTable("diphonelicense")
	diphonelicense_schemardd.insertInto("diphonelicense",overwrite=True)	

def divin_phoneGet(sc,sqlContext):
	vinphone = getfixDi(sqlContext,HDFS_PATH_TT_VIN_PHONE,VP_SYSMAP,VP_CMAP)
	rdd = getDiMulToDiMul(vinphone).coalesce(60)
	divinphone_schemardd = sqlContext.applySchema(rdd,divinphone_rdd_schema)
	divinphone = sqlContext.parquetFile(HDFS_PATH_DI_VIN_PHONE)
	divinphone.registerTempTable("divin_phone")
	divinphone_schemardd.insertInto("divin_phone",overwrite=True)	

def divin_licenseGet(sc,sqlContext):
	vinlicense = getfixDi(sqlContext,HDFS_PATH_TT_VIN_LICENSE,VL_SYSMAP,VL_CMAP)
	rdd = getDiOneToOne(vinlicense).coalesce(100)
	divinlicense_schemardd = sqlContext.applySchema(rdd,divinlicense_rdd_schema)
	divinlicense = sqlContext.parquetFile(HDFS_PATH_DI_VIN_LICENSE)
	divinlicense.registerTempTable("divinlicense")
	divinlicense_schemardd.insertInto("divinlicense",overwrite=True)	

if __name__ == '__main__':
	diphone_licenseGet(sc,sqlContext)
	divin_phoneGet(sc,sqlContext)	
	divin_licenseGet(sc,sqlContext)	


