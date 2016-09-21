# -*- coding: utf-8 -*-
from __future__ import division
import sys
import datetime
import math

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_DI")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.superid_DiPR.dischema import *
from mathartsys.supcap.superid.superid_DiPR.dipath import *
from mathartsys.supcap.superid.superid_DiPR.superid_map import *
from mathartsys.supcap.superid.superid_DiPR.superid_function import *
from mathartsys.supcap.superid.key_key_table.key_tablepath import *

def diphone_weiboGet(sc,sqlContext):
	phoneweibo = getfixDi(sqlContext,HDFS_PATH_TT_PHONE_WEIBO,PWB_SYSMAP,PWB_CMAP)
	rdd = getDiOneToMul(phoneweibo,1).coalesce(10)
	diphoneweibo_schemardd = sqlContext.applySchema(rdd,diphoneweibo_rdd_schema)
	diphoneweibo= sqlContext.parquetFile(HDFS_PATH_DI_PHONE_WEIBO)
	diphoneweibo.registerTempTable("diphoneweibo")
	diphoneweibo_schemardd.insertInto("diphoneweibo",overwrite=True)	

def dirid_weiboGet(sc,sqlContext):
	ridweibo = getfixDi(sqlContext,HDFS_PATH_TT_RID_WEIBO,RWB_SYSMAP,RWB_CMAP)
	rdd = getDiOneToMul(ridweibo,1).coalesce(10).coalesce(10)
	diridweibo_schemardd = sqlContext.applySchema(rdd,diridweibo_rdd_schema)
	diridweibo = sqlContext.parquetFile(HDFS_PATH_DI_RID_WEIBO)
	diridweibo.registerTempTable("diridweibo")
	diridweibo_schemardd.insertInto("diridweibo",overwrite=True)	

def divin_weiboGet(sc,sqlContext):
	vinweibo = getfixDi(sqlContext,HDFS_PATH_TT_VIN_WEIBO,VWB_SYSMAP,VWB_CMAP)
	rdd = getDiOneToMul(vinweibo,1).coalesce(10)
	divinweibo_schemardd = sqlContext.applySchema(rdd,divinweibo_rdd_schema)
	divinweibo = sqlContext.parquetFile(HDFS_PATH_DI_VIN_WEIBO)
	divinweibo.registerTempTable("divinweibo")
	divinweibo_schemardd.insertInto("divinweibo",overwrite=True)	

def diphone_emailGet(sc,sqlContext):
	phoneemail = getfixDi(sqlContext,HDFS_PATH_TT_PHONE_EMAIL,PE_SYSMAP,PE_CMAP)
	rdd = getDiOneToMul(phoneemail,1).coalesce(100)
	diphoneemail_schemardd = sqlContext.applySchema(rdd,diphoneemail_rdd_schema)
	diphoneemail = sqlContext.parquetFile(HDFS_PATH_DI_PHONE_EMAIL)
	diphoneemail.registerTempTable("diphoneemail")
	diphoneemail_schemardd.insertInto("diphoneemail",overwrite=True)	

def divin_emailGet(sc,sqlContext):
	vinemail = getfixDi(sqlContext,HDFS_PATH_TT_VIN_EMAIL,VE_SYSMAP,VE_CMAP)
	rdd = getDiMulToDiMul(vinemail).coalesce(30)
	divinemail_schemardd = sqlContext.applySchema(rdd,divinemail_rdd_schema)
	divinemail = sqlContext.parquetFile(HDFS_PATH_DI_VIN_EMAIL)
	divinemail.registerTempTable("divinemail")
	divinemail_schemardd.insertInto("divinemail",overwrite=True)	

def dirid_emailGet(sc,sqlContext):
	ridemail = getfixDi(sqlContext,HDFS_PATH_TT_RID_EMAIL,RE_SYSMAP,RE_CMAP)
	rdd = getDiOneToMul(ridemail,1).coalesce(60)
	diridemail_schemardd = sqlContext.applySchema(rdd,diridemail_rdd_schema)
	diridemail = sqlContext.parquetFile(HDFS_PATH_DI_RID_EMAIL)
	diridemail.registerTempTable("diridemail")
	diridemail_schemardd.insertInto("diridemail",overwrite=True)	

def diphone_weixinGet(sc,sqlContext):
	phoneweixin = getfixDi(sqlContext,HDFS_PATH_TT_PHONE_WEIXIN,PWX_SYSMAP,PWX_CMAP)
	rdd = getDiOneToOne(phoneweixin).coalesce(10)
	diphoneweixin_schemardd = sqlContext.applySchema(rdd,diphoneweixin_rdd_schema)
	diphoneweixin = sqlContext.parquetFile(HDFS_PATH_DI_PHONE_WEIXIN)
	diphoneweixin.registerTempTable("diphoneweixin")
	diphoneweixin_schemardd.insertInto("diphoneweixin",overwrite=True)	

def dirid_weixinGet(sc,sqlContext):
	ridweixin = getfixDi(sqlContext,HDFS_PATH_TT_RID_WEIXIN,RWX_SYSMAP,RWX_CMAP)
	rdd = getDiOneToMul(ridweixin,1).coalesce(10)
	diridweixin_schemardd = sqlContext.applySchema(rdd,diridweixin_rdd_schema)
	diridweixin = sqlContext.parquetFile(HDFS_PATH_DI_RID_WEIXIN)
	diridweixin.registerTempTable("diridweixin")
	diridweixin_schemardd.insertInto("diridweixin",overwrite=True)	

def divin_weixinGet(sc,sqlContext):
	vinweixin = getfixDi(sqlContext,HDFS_PATH_TT_VIN_WEIXIN,VWX_SYSMAP,VWX_CMAP)
	rdd = getDiOneToMul(vinweixin,1).coalesce(10)
	divinweixin_schemardd = sqlContext.applySchema(rdd,divinweixin_rdd_schema)
	divinweixin = sqlContext.parquetFile(HDFS_PATH_DI_VIN_WEIXIN)
	divinweixin.registerTempTable("divinweixin")
	divinweixin_schemardd.insertInto("divinweixin",overwrite=True)	

def dilicense_emailGet(sc,sqlContext):
	licenseemail = getfixDi(sqlContext,HDFS_PATH_TT_LICENSE_EMAIL,LE_SYSMAP,LE_CMAP)
	rdd = getDiMulToDiMul(licenseemail).coalesce(10)
	dilicenseemail_schemardd = sqlContext.applySchema(rdd,dilicenseemail_rdd_schema)
	dilicenseemail = sqlContext.parquetFile(HDFS_PATH_DI_LICENSE_EMAIL)
	dilicenseemail.registerTempTable("dilicenseemail")
	dilicenseemail_schemardd.insertInto("dilicenseemail",overwrite=True)	

def dilicense_weixinGet(sc,sqlContext):
	licenseweixin = getfixDi(sqlContext,HDFS_PATH_TT_LICENSE_WEIXIN,WL_SYSMAP,WL_CMAP)
	rdd = getDiMulToDiMul(licenseweixin).coalesce(10)
	dilicenseweixin_schemardd = sqlContext.applySchema(rdd,dilicenseweixin_rdd_schema)
	dilicenseweixin = sqlContext.parquetFile(HDFS_PATH_DI_LICENSE_WEIXIN)
	dilicenseweixin.registerTempTable("dilicenseweixin")
	dilicenseweixin_schemardd.insertInto("dilicenseweixin",overwrite=True)	

def dirid_vinGet(sc,sqlContext):
	ridvin = getfixDi(sqlContext,HDFS_PATH_TT_VIN_RID,VR_SYSMAP,VR_CMAP)
	rdd = getDiOneToMul(ridvin,1).coalesce(60)
	diridvin_schemardd = sqlContext.applySchema(rdd,divinrid_rdd_schema)
	diridvin = sqlContext.parquetFile(HDFS_PATH_DI_RID_VIN)
	diridvin.registerTempTable("diridvin")
	diridvin_schemardd.insertInto("diridvin",overwrite=True)	

def dirid_phoneGet(sc,sqlContext):
	ridphone = getfixDi(sqlContext,HDFS_PATH_TT_PHONE_RID,RP_SYSMAP,RP_CMAP)
	rdd = getDiOneToMul(ridphone,1).coalesce(60)
	diridphone_schemardd = sqlContext.applySchema(rdd,diphonerid_rdd_schema)
	diridphone = sqlContext.parquetFile(HDFS_PATH_DI_RID_PHONE)
	diridphone.registerTempTable("diridphone")
	diridphone_schemardd.insertInto("diridphone",overwrite=True)

def dirid_licenseGet(sc,sqlContext):
	ridlicense = getfixDi(sqlContext,HDFS_PATH_TT_LICENSE_RID,RL_SYSMAP,RL_CMAP)
	rdd = getDiOneToMul(ridlicense,1).coalesce(60)
	diridlicense_schemardd = sqlContext.applySchema(rdd,dilicenserid_rdd_schema)
	diridlicense = sqlContext.parquetFile(HDFS_PATH_DI_RID_LICENSE)
	diridlicense.registerTempTable("diridlicense")
	diridlicense_schemardd.insertInto("diridlicense",overwrite=True)

if __name__ == '__main__':
	diphone_weiboGet(sc,sqlContext)
	dirid_weiboGet(sc,sqlContext)
	divin_weiboGet(sc,sqlContext)
	diphone_emailGet(sc,sqlContext)
	divin_emailGet(sc,sqlContext)
	dirid_emailGet(sc,sqlContext)
	diphone_weixinGet(sc,sqlContext)
	dilicense_emailGet(sc,sqlContext)	
	dirid_weixinGet(sc,sqlContext)	
	divin_weixinGet(sc,sqlContext)
	dilicense_weixinGet(sc,sqlContext)	
	dirid_vinGet(sc,sqlContext)
	dirid_phoneGet(sc,sqlContext)
	dirid_licenseGet(sc,sqlContext)


		