# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_KEY_KEY")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.key_key_table.key_tablepath import *
from mathartsys.supcap.superid.key_key_table.key_function import *
from mathartsys.supcap.superid.key_key_table.key_schema import *
from mathartsys.supcap.superid.key_key_table.sitemonitor_report_new_table import *
from mathartsys.supcap.superid.key_key_table.cx_asset_tt_table import *
from mathartsys.supcap.superid.key_key_table.user_attributes_table import *
from mathartsys.supcap.superid.key_key_table.s_srv_req_table import*


# def getPhoneCookie(sc,sqlContext,dt,interval=0):
# 	try:
# 		phonecookie_temp_rdd = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_MZCOOKIE).map(lambda x: (x[:3],(long[x[3]]),long(x[4]),float(x[5])))
# 	except:
# 		phonecookie_temp_rdd = sc.parallelize([])
# 	phonecookie_rdd = getSequnceFileBydt(sc,HDFS_PATH_SITEMONITOR_REPORT_NEW,dt,interval).filter(lambda x : len(x[MZ_HOPNE]) == 11 and len(x[MZ_MZ_COOKIE]) == 12) \
# 						.map(lambda x: ((x[MZ_HOPNE],x[MZ_MZ_COOKIE],'MZ'),(1L,x[MZ_DT],1.0))) \
# 						.union(phonecookie_temp_rdd) \
# 						.reduceByKey(lambda x,y : (long(x[0]+y[0]),max(x[1],y[1]),max(x[2],y[2]))).map(lambda x: x[0]+x[1])
# 	phonecookie_rdd_schemardd = sqlContext.applySchema(phonecookie_rdd,phonecookie_rdd_schema).coalesce(20)
# 	phonecookie = SQLContextxt.parquetFile(HDFS_PATH_TT_PHONE_MZCOOKIE)
# 	phonecookie.registerTempTable("phonecookie")				  
# 	phonecookie_rdd_schemardd.insertInto("phonecookie",overwrite=True)

def getPhoneWeibo(sc,sqlContext):
	phoneweibo_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_WEIBO).map(lambda x: ((x[1],x[0],x[2]),(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	phoneweibo_rdd = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_WEIBO] not in ('Null','NULL','null','')) \
						.map(lambda x: ((x[CX_WEIBO],x[CX_LAST_UPD]),getDefaultDi((x[CX_PRI_PHONE_NUM],x[CX_WORK_PHONE],x[CX_PRI_CONT_PHONE_NUM])))) \
						.flatMapValues(lambda x:x).filter (lambda x: len(x[1][0]) == 11) \
						.map(lambda x: ((x[0][0],x[1][0],'CEM'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1]))) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(phoneweibo_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][1],x[0][0],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phoneweibo_rdd_schemardd = sqlContext.applySchema(phoneweibo_rdd,phoneweibo_rdd_schema).coalesce(10)
	phoneweibo = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_WEIBO)
	phoneweibo.registerTempTable("phoneweibo")				  
	phoneweibo_rdd_schemardd.insertInto("phoneweibo",overwrite=True)

def getRidWeibo(sc,sqlContext):
	ridweibo_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_RID_WEIBO).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	ridweibo_rdd = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_WEIBO] not in ('Null','NULL','null','') and len(x[CX_RID]) in (15,18) and x[CX_RID][6:10] != '1111') \
						.map(lambda x: ((x[CX_RID],x[CX_WEIBO],'CEM'),(1L,datetime.datetime.strptime(x[CX_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0))) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(ridweibo_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	ridweibo_rdd_schemardd = sqlContext.applySchema(ridweibo_rdd,ridweibo_rdd_schema).coalesce(10)
	ridweibo = sqlContext.parquetFile(HDFS_PATH_TT_RID_WEIBO)
	ridweibo.registerTempTable("ridweibo")				  
	ridweibo_rdd_schemardd.insertInto("ridweibo",overwrite=True)

def getVinWeibo(sc,sqlContext):
	vinweibo_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_WEIBO).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinweibo_rdd = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_WEIBO] not in ('Null','NULL','null','') and x[CX_VIN] != 'null') \
						.map(lambda x: ((x[CX_VIN],x[CX_WEIBO],'CEM'),(1L,datetime.datetime.strptime(x[CX_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0))) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinweibo_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinweibo_rdd_schemardd = sqlContext.applySchema(vinweibo_rdd,vinweibo_rdd_schema).coalesce(10)
	vinweibo = sqlContext.parquetFile(HDFS_PATH_TT_VIN_WEIBO)
	vinweibo.registerTempTable("vinweibo")				  
	vinweibo_rdd_schemardd.insertInto("vinweibo",overwrite=True)

def getPhoneEmail(sc,sqlContext):
	today = datetime.datetime.today()
	phoneemail_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_EMAIL).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	phoneemail_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.PHONE_NO,x.EMAIL,'IDM'),(1L,today,1.0))).filter(lambda x: x[0][0] and x[0][1]) 
						# .reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phoneemail_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE).map(lambda x: ((x.EMAIL,x.TIME),getDefaultDi((x.PHONE1,x.PHONE2,x.PHONE3,x.PHONE4)))) \
						.flatMapValues(lambda x: x).map(lambda x: ((x[1][0],x[0][0],'DOSS'),(1,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][-1]))).filter(lambda x: (len(x[0][0]) == 11 and x[0][1] != 'null' and x[0][0][0] == '1')) 
						# .reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: x[0] + (x[1][0],x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phoneemail_rdd = phoneemail_idm.union(phoneemail_doss).reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(phoneemail_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: x[0] + (long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phoneemail_rdd_schemardd = sqlContext.applySchema(phoneemail_rdd,phoneemail_rdd_schema).coalesce(10)
	phoneemail = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_EMAIL)
	phoneemail.registerTempTable("phoneemail")				  
	phoneemail_rdd_schemardd.insertInto("phoneemail",overwrite=True)

def getVinEmail(sc,sqlContext):
	today = datetime.datetime.today()
	vinemail_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_EMAIL).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinemail_idm_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.EMAIL),getDefaultDi((x.VIN1,x.VIN2)))).flatMapValues(lambda x: x) \
						.map(lambda x: ((x[1][0],x[0],'IDM'),(1L,today,x[1][1]))).filter(lambda x: x[0][0] and x[0][1])
	vinemail_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE) \
						.map(lambda x: ((x.VIN,x.EMAIL,'DOSS'),(1,datetime.datetime.strptime(x.TIME,"%Y-%m-%d %H:%M:%S.0"),1.0))).filter(lambda x: (x[0][0] != 'null' and x[0][1] != 'null')) 					
	vinemail_rdd = vinemail_idm_rdd.union(vinemail_doss).reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinemail_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinemail_rdd_schemardd = sqlContext.applySchema(vinemail_rdd,vinemail_rdd_schema).coalesce(10)
	vinemail = sqlContext.parquetFile(HDFS_PATH_TT_VIN_EMAIL)
	vinemail.registerTempTable("vinemail")				  
	vinemail_rdd_schemardd.insertInto("vinemail",overwrite=True)

def getRidEmail(sc,sqlContext):
	today = datetime.datetime.today()
	ridemail_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_RID_EMAIL).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	ridemail_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.RID,x.EMAIL,'IDM'),(1L,today,1.0))).filter(lambda x: x[0][0] and x[0][1]) 
	ridemail_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE) \
						.map(lambda x: ((x.RID,x.EMAIL,'DOSS'),(1,datetime.datetime.strptime(x.TIME,"%Y-%m-%d %H:%M:%S.0"),1.0))).filter(lambda x: (x[0][0] != 'null' and x[0][1] != 'null')) 					
	ridemail_rdd = ridemail_idm.union(ridemail_doss).reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(ridemail_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	ridemail_rdd_schemardd = sqlContext.applySchema(ridemail_rdd,ridemail_rdd_schema).coalesce(10)
	ridemail = sqlContext.parquetFile(HDFS_PATH_TT_RID_EMAIL)
	ridemail.registerTempTable("ridemail")				  
	ridemail_rdd_schemardd.insertInto("ridemail",overwrite=True)

def getLicenseEmail(sc,sqlContext):
	today = datetime.datetime.today()
	licenseemail_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_EMAIL).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	licenseemail_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.filter(lambda x: x.LICENSE and x.EMAIL) \
						.map(lambda x: ((x.LICENSE.replace(' ','').replace('-','').replace('/',''),x.EMAIL,'IDM'),(1L,today,1.0))) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(licenseemail_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	licenseemail_rdd_schemardd = sqlContext.applySchema(licenseemail_rdd,licenseemail_rdd_schema).coalesce(10)
	licenseemail = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_EMAIL)
	licenseemail.registerTempTable("licenseemail")				  
	licenseemail_rdd_schemardd.insertInto("licenseemail",overwrite=True)

def getPhoneWeixin(sc,sqlContext):
	today = datetime.datetime.today()
	phoneweixin_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_WEIXIN).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	phoneweixin_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.PHONE_NO,x.WEIXIN,'IDM'),(1L,today,1.0))).filter(lambda x: x[0][0] and x[0][1]) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(phoneweixin_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phoneweixin_rdd_schemardd = sqlContext.applySchema(phoneweixin_rdd,phoneweixin_rdd_schema).coalesce(10)
	phoneweixin = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_WEIXIN)
	phoneweixin.registerTempTable("phoneweixin")				  
	phoneweixin_rdd_schemardd.insertInto("phoneweixin",overwrite=True)

def getRidWeixin(sc,sqlContext):
	today = datetime.datetime.today()
	ridweixin_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_RID_WEIXIN).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	ridweixin_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.RID,x.WEIXIN,'IDM'),(1L,today,1.0))).filter(lambda x: x[0][0] and x[0][1]) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(ridweixin_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2]))) 	
	ridweixin_rdd_schemardd = sqlContext.applySchema(ridweixin_rdd,ridweixin_rdd_schema).coalesce(10)
	ridweixin = sqlContext.parquetFile(HDFS_PATH_TT_RID_WEIXIN)
	ridweixin.registerTempTable("ridweixin")				  
	ridweixin_rdd_schemardd.insertInto("ridweixin",overwrite=True)

def getVinWeixin(sc,sqlContext):
	today = datetime.datetime.today()
	vinweixin_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_WEIXIN).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinweixin_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.WEIXIN),getDefaultDi((x.VIN1,x.VIN2)))).flatMapValues(lambda x: x) \
						.map(lambda x: ((x[1][0],x[0],'IDM'),(1L,today,x[1][1]))).filter(lambda x: x[0][0] and x[0][1]) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinweixin_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinweixin_rdd_schemardd = sqlContext.applySchema(vinweixin_rdd,vinweixin_rdd_schema).coalesce(10)
	vinweixin = sqlContext.parquetFile(HDFS_PATH_TT_VIN_WEIXIN)
	vinweixin.registerTempTable("vinweixin")				  
	vinweixin_rdd_schemardd.insertInto("vinweixin",overwrite=True)

def getLicenseWeixin(sc,sqlContext):
	today = datetime.datetime.today()
	licenseweixin_rdd_old = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_WEIXIN).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	licenseweixin_rdd = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.filter(lambda x: x.LICENSE and x.WEIXIN) \
						.map(lambda x: ((x.LICENSE.replace(' ','').replace('-',''),x.WEIXIN,'IDM'),(1L,today,1.0))) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(licenseweixin_rdd_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))						
	licenseweixin_rdd_schemardd = sqlContext.applySchema(licenseweixin_rdd,licenseweixin_rdd_schema).coalesce(10)
	licenseweixin = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_WEIXIN)
	licenseweixin.registerTempTable("licenseweixin")				  
	licenseweixin_rdd_schemardd.insertInto("licenseweixin",overwrite=True)

def getVinRid(sc,sqlContext):
	today = datetime.datetime.today()
	vinrid_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_RID).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinrid_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.RID),getDefaultDi((x.VIN1,x.VIN2)))).flatMapValues(lambda x: x) \
						.map(lambda x: ((x[1][0],x[0],'IDM'),(1L,today,x[1][1]))).filter(lambda x: x[0][0] and x[0][1])
	vinrid_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_VIN] not in ('Null','NULL','null','') and len(x[CX_RID]) in (15,18) and x[CX_RID][6:10] != '1111') \
						.map(lambda x: ((x[CX_VIN],x[CX_RID],'CEM'),(1L,datetime.datetime.strptime(x[CX_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0))) 
	vinrid_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE) \
						.map(lambda x: ((x.VIN,x.RID,'DOSS'),(1,datetime.datetime.strptime(x.TIME,"%Y-%m-%d %H:%M:%S.0"),1.0))).filter(lambda x: (x[0][0] != 'null' and x[0][1] != 'null')) 					
	vinrid_rdd = vinrid_idm.union(vinrid_cem).union(vinrid_doss) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinrid_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinrid_rdd_schemardd = sqlContext.applySchema(vinrid_rdd,vinrid_rdd_schema).coalesce(50)
	vinrid = sqlContext.parquetFile(HDFS_PATH_TT_VIN_RID)
	vinrid.registerTempTable("vinrid")				  
	vinrid_rdd_schemardd.insertInto("vinrid",overwrite=True)

def getPhoneRid(sc,sqlContext):
	today = datetime.datetime.today()
	phonerid_old = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_RID).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	phonerid_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.PHONE_NO,x.RID,'IDM'),(1L,today,1.0))).filter(lambda x: x[0][0] and x[0][1]) 
	phonerid_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: len(x[CX_RID]) in (15,18) and x[CX_RID][6:10] != '1111') \
						.map(lambda x: ((x[CX_RID],x[CX_LAST_UPD]),getDefaultDi((x[CX_PRI_PHONE_NUM],x[CX_WORK_PHONE],x[CX_PRI_CONT_PHONE_NUM])))) \
						.flatMapValues(lambda x:x).filter (lambda x: len(x[1][0]) == 11) \
						.map(lambda x: ((x[1][0],x[0][0],'CEM'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1]))) 
	phonerid_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE).map(lambda x: ((x.RID,x.TIME),getDefaultDi((x.PHONE1,x.PHONE2,x.PHONE3,x.PHONE4)))) \
						.flatMapValues(lambda x: x).map(lambda x: ((x[1][0],x[0][0],'DOSS'),(1,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][-1]))).filter(lambda x: (len(x[0][0]) == 11 and x[0][1] != 'null' and x[0][0][0] == '1')) 
	phonerid_rdd = phonerid_idm.union(phonerid_cem).union(phonerid_doss) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(phonerid_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phonerid_rdd_schemardd = sqlContext.applySchema(phonerid_rdd,phonerid_rdd_schema).coalesce(50)
	phonerid = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_RID)
	phonerid.registerTempTable("phonerid")				  
	phonerid_rdd_schemardd.insertInto("phonerid",overwrite=True)

def getLicenseRid(sc,sqlContext):
	today = datetime.datetime.today()
	licenserid_old = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_RID).map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	licenserid_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.filter(lambda x: x.LICENSE and x.RID) \
						.map(lambda x: ((x.LICENSE.replace(' ','').replace('-',''),x.RID,'IDM'),(1L,today,1.0)))
	licenserid_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_LCNS_NUM] not in ('Null','NULL','null','') and len(x[CX_RID]) in (15,18) and x[CX_RID][6:10] != '1111') \
						.map(lambda x: ((x[CX_LCNS_NUM].replace(' ','').replace('-',''),x[CX_RID],'CEM'),(1L,datetime.datetime.strptime(x[CX_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0)))
	licenserid_rdd = licenserid_idm.union(licenserid_cem) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(licenserid_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	licenserid_rdd_schemardd = sqlContext.applySchema(licenserid_rdd,licenserid_rdd_schema).coalesce(50)
	licenserid = sqlContext.parquetFile(HDFS_PATH_TT_LICENSE_RID)
	licenserid.registerTempTable("licenserid")				  
	licenserid_rdd_schemardd.insertInto("licenserid",overwrite=True)

def getVinPhone(sc,sqlContext):
	today = datetime.datetime.today()
	vinphone_cemidm_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_PHONE).filter(lambda x: x.SYSTEM != 'DATASTORE').map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinphone_ds = sqlContext.parquetFile(HDFS_PATH_REPAIR_ORDER_TEMP_TABLE) \
						.map(lambda x: ((x.VIN,x.TIME),getDefaultDi((x.MOBILE,x.PNONE)))) \
						.flatMapValues(lambda x: x) \
						.filter(lambda x: x[0][0] != 'null' and len(x[1][0]) == 11) \
						.map(lambda x: ((x[0][0],x[1][0],'DATASTORE'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1]))) \
						.reduceByKey(lambda x,y : (long(x[0]+y[0]),max(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinphone_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_VIN] not in ('Null','NULL','null','')) \
						.map(lambda x: ((x[CX_VIN],x[CX_LAST_UPD]),getDefaultDi((x[CX_PRI_PHONE_NUM],x[CX_WORK_PHONE],x[CX_PRI_CONT_PHONE_NUM])))) \
						.flatMapValues(lambda x:x).filter (lambda x: len(x[1][0]) == 11) \
						.map(lambda x: ((x[0][0],x[1][0],'CEM'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1]))) 
	vinphone_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.map(lambda x: ((x.PHONE_NO),getDefaultDi((x.VIN1,x.VIN2)))).flatMapValues(lambda x: x) \
						.map(lambda x: ((x[1][0],x[0],'IDM'),(1L,today,x[1][1]))).filter(lambda x: x[0][0] and x[0][1]) 
	vinphone_doss = sqlContext.parquetFile(HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE).map(lambda x: ((x.VIN,x.TIME),getDefaultDi((x.PHONE1,x.PHONE2,x.PHONE3,x.PHONE4)))) \
						.flatMapValues(lambda x: x).map(lambda x: ((x[0][0],x[1][0],'DOSS'),(1,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][-1]))).filter(lambda x: (x[0][0] != 'null' and len(x[0][1]) == 11)) 
	vinphone_rdd = vinphone_cem.union(vinphone_idm).union(vinphone_doss) \
						.reduceByKey(lambda x,y : (x[0]+y[0],min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinphone_cemidm_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2]))) \
						.union(vinphone_ds)
	vinphone_rdd_schemardd = sqlContext.applySchema(vinphone_rdd,vinphone_rdd_schema).coalesce(300)
	vinphone = sqlContext.parquetFile(HDFS_PATH_TT_VIN_PHONE)
	vinphone.registerTempTable("vinphone")				  
	vinphone_rdd_schemardd.insertInto("vinphone",overwrite=True)

def getVinLicense(sc,sqlContext):
	today = datetime.datetime.today()
	vinlicense_cemidm_old = sqlContext.parquetFile(HDFS_PATH_TT_VIN_LICENSE).filter(lambda x: x.SYSTEM != 'DATASTORE').map(lambda x: (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	vinlicense_ds = sqlContext.parquetFile(HDFS_PATH_REPAIR_ORDER_TEMP_TABLE) \
						.filter(lambda x: len(x.VIN) >10  and x.LICENSE != 'null') \
						.map(lambda x: ((x.VIN,x.LICENSE.replace(' ','').replace('-',''),'DATASTORE'),(1L,datetime.datetime.strptime(x.TIME,"%Y-%m-%d %H:%M:%S.0"),1.0))) \
						.reduceByKey(lambda x,y : (long(x[0]+y[0]),max(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	vinlicense_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_VIN] not in ('Null','NULL','null','') and x[CX_LCNS_NUM] != 'null') \
						.map(lambda x: ((x[CX_VIN],x[CX_LCNS_NUM].replace(' ','').replace('-',''),'CEM'),(1L,datetime.datetime.strptime(x[CX_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0)))
	vinlicense_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.filter(lambda x: x.LICENSE and (x.VIN1 or x.VIN2)) \
						.map(lambda x: ((x.LICENSE.replace(' ','').replace('-','')),getDefaultDi((x.VIN1,x.VIN2)))).flatMapValues(lambda x: x) \
						.map(lambda x: ((x[1][0],x[0],'IDM'),(1L,today,x[1][1])))
	vinlicense_rdd = vinlicense_cem.union(vinlicense_idm) \
						.reduceByKey(lambda x,y : (1L,min(x[1],y[1]),max(x[2],y[2]))) \
						.union(vinlicense_cemidm_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2]))) \
						.union(vinlicense_ds)
	vinlicense_rdd_schemardd = sqlContext.applySchema(vinlicense_rdd,vinlicense_rdd_schema).repartition(300)
	vinlicense = sqlContext.parquetFile(HDFS_PATH_TT_VIN_LICENSE)
	vinlicense.registerTempTable("vinlicense")				  
	vinlicense_rdd_schemardd.insertInto("vinlicense",overwrite=True)

def getPhoneLicense(sc,sqlContext):
	today = datetime.datetime.today()
	phonelicense_cemidm_old = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_LICENSE).filter(lambda x: x.SYSTEM in ('IDM','CEM')).map(lambda x : (x[:3],(x[3],datetime.datetime.strptime(x[4],"%Y-%m-%d"),x[5])))
	phonelicense_ds = sqlContext.parquetFile(HDFS_PATH_REPAIR_ORDER_TEMP_TABLE) \
						.map(lambda x: ((x.LICENSE.replace(' ','').replace('-',''),x.TIME),getDefaultDi((x.MOBILE,x.PNONE)))) \
						.flatMapValues(lambda x: x) \
						.filter(lambda x: x[0][0] != 'null' and len(x[1][0]) == 11) \
						.map(lambda x: ((x[1][0],x[0][0],'DATASTORE'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1]))) \
						.reduceByKey(lambda x,y : (long(x[0]+y[0]),max(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phonelicense_cem = sc.textFile(HDFS_PATH_CX_ASSET_TT).map(lambda x: x.split("\x01")).filter(lambda x: x[CX_LCNS_NUM] not in ('Null','NULL','null','')) \
						.map(lambda x: ((x[CX_LCNS_NUM].replace(' ','').replace('-',''),x[CX_LAST_UPD]),getDefaultDi((x[CX_PRI_PHONE_NUM],x[CX_WORK_PHONE],x[CX_PRI_CONT_PHONE_NUM])))) \
						.flatMapValues(lambda x:x).filter (lambda x: len(x[1][0]) == 11) \
						.map(lambda x: ((x[1][0],x[0][0],'CEM'),(1L,datetime.datetime.strptime(x[0][1],"%Y-%m-%d %H:%M:%S.0"),x[1][1])))
	phonelicense_idm = sqlContext.parquetFile(HDFS_PATH_IDM_TEMP_TABLE) \
						.filter(lambda x: x.PHONE_NO and x.LICENSE) \
						.map(lambda x: ((x.PHONE_NO,x.LICENSE.replace(' ','').replace('-',''),'IDM'),(1L,today,1.0)))
	phonelicense_cac = sc.textFile(HDFS_PATH_TT_S_SRV_REQ).map(lambda x: x.split("\x01")) \
						.filter(lambda x: x[S_X_CAC_LCNS_NUM] not in ('Null','NULL','null','') and len(x[S_ALT_CON_PH_NUM]) == 11 and x[S_LAST_UPD] not in ('Null','NULL','null','')) \
						.map(lambda x: ((x[S_ALT_CON_PH_NUM],x[S_X_CAC_LCNS_NUM].replace(' ','').replace('-',''),'CAC'),(1L,datetime.datetime.strptime(x[S_LAST_UPD],"%Y-%m-%d %H:%M:%S.0"),1.0))) \
						.reduceByKey(lambda x,y : (long(x[0]+y[0]),max(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2])))
	phonelicense_rdd = phonelicense_cem.union(phonelicense_idm) \
						.reduceByKey(lambda x,y : (1L,min(x[1],y[1]),max(x[2],y[2]))) \
						.union(phonelicense_cemidm_old) \
						.reduceByKey(lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),max(x[2],y[2]))).map(lambda x: (x[0][0],x[0][1],x[0][2],long(x[1][0]),x[1][1].strftime("%Y-%m-%d"),float(x[1][2]))) \
						.union(phonelicense_ds).union(phonelicense_cac)
	phonelicense_rdd_schemardd = sqlContext.applySchema(phonelicense_rdd,phonelicense_rdd_schema).coalesce(600)
	phonelicense = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_LICENSE)
	phonelicense.registerTempTable("phonelicense")				  
	phonelicense_rdd_schemardd.insertInto("phonelicense",overwrite=True)

def getTotalTable(sc,sqlContext):
	# today = datetime.datetime.today()
	# try:
	# 	dt = sys.argv[1]
	# 	interval = int(sys.argv[2])
	# except:
	# 	today = datetime.datetime.today()
	# 	dt = '20141110'
	# 	interval = (today-datetime.datetime.strptime(dt,"%Y%m%d")).days
	# try:
	# 	getPhoneCookie(dt,interval)
	# except Exception, e:
	# 	print "getPhoneCookie failed ================="
	try:
		getVinRid(sc,sqlContext)
	except Exception, e:
		print "getVinRid failed ================="
	try:
		getPhoneRid(sc,sqlContext)
	except Exception, e:
		print "getPhoneRid failed ================="
	try:
		getPhoneLicense(sc,sqlContext)
	except Exception, e:
		print "getPhoneLicense failed ================="	
	try:
		getVinLicense(sc,sqlContext)
	except Exception, e:
		print "getVinLicense failed ================="
	try:
		getVinPhone(sc,sqlContext)
	except Exception, e:
		print "getVinPhone failed ================="
	try:
		getPhoneWeibo(sc,sqlContext)
	except Exception, e:
		print "getPhoneWeibo failed ================="
	try:
		getRidWeibo(sc,sqlContext)
	except Exception, e:
		print "getRidWeibo failed ================="
	try:
		getVinWeibo(sc,sqlContext)
	except Exception, e:
		print "getVinWeibo failed ================="
	try:
		getPhoneEmail(sc,sqlContext)
	except Exception, e:
		print "getPhoneEmail failed ================="
	try:
		getVinEmail(sc,sqlContext)
	except Exception, e:
		print "getVinEmail failed ================="
	try:
		getRidEmail(sc,sqlContext)
	except Exception, e:
		print "getRidEmail failed ================="
	try:
		getLicenseEmail(sc,sqlContext)
	except Exception, e:
		print "getLicenseEmail failed ================="
	try:
		getPhoneWeixin(sc,sqlContext)
	except Exception, e:
		print "getPhoneWeixin failed ================="
	try:
		getRidWeixin(sc,sqlContext)
	except Exception, e:
		print "getRidWeixin failed ================="
	try:
		getVinWeixin(sc,sqlContext)
	except Exception, e:
		print "getVinWeixin failed ================="
	try:
		getLicenseWeixin(sc,sqlContext)
	except Exception, e:
		print "getLicenseWeixin failed ================="
	try:
		getLicenseRid(sc,sqlContext)
	except Exception, e:
		print "getLicenseRid failed ================="

if __name__ == '__main__':
	getTotalTable(sc,sqlContext)