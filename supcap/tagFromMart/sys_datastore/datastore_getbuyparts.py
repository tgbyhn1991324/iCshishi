# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_init")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tablepath import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_function import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_schema import *

def getBuyPartsTag(sc,sqlContext):
	today = datetime.datetime.today()
	repair_part_pf = sqlContext.parquetFile(HDFS_PATH_DT_BO_REPAIR_PART_TOTAL)			
	union_rdd = repair_part_pf.map(lambda x: (x.vin,(x.start_time,x.part_quantity,x.part_sale_amount)))
	#(u'LSGJA52U59S124382', (u'2012-12-13 00:00:00.0', u'1', u'280')
	union_rdd.persist()
	#12个月购买养护品标签
	buyparts_lm12 = union_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][0],"%Y-%m-%d %H:%M:%S.0")).days<=365) \
					.aggregateByKey((0,0,0),lambda x,y: (x[0]+1,x[1]+float(y[1]),x[2]+(float(y[2]))),lambda x,y:(x[0]+y[0],x[1]+y[1],x[2]+y[2])).map(lambda x: (x[0],int(x[1][0]),int(x[1][1]),int(x[1][2])))
	buyparts_lm12_schemardd = sqlContext.applySchema(buyparts_lm12,buyparts_lm12_schema).repartition(50)
	buypasrs_lm12 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM12)
	buypasrs_lm12.registerTempTable("buypasrs_lm12")				  
	buyparts_lm12_schemardd.insertInto("buypasrs_lm12",overwrite=True)
	#6个月购买养护品标签
	buyparts_lm6 = union_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][0],"%Y-%m-%d %H:%M:%S.0")).days<=180) \
					.aggregateByKey((0,0,0),lambda x,y: (x[0]+1,x[1]+float(y[1]),x[2]+(float(y[2]))),lambda x,y:(x[0]+y[0],x[1]+y[1],x[2]+y[2])).map(lambda x: (x[0],int(x[1][0]),int(x[1][1]),int(x[1][2])))
	buyparts_lm6_schemardd = sqlContext.applySchema(buyparts_lm6,buyparts_lm6_schema).repartition(50)
	buypasrs_lm6 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM6)
	buypasrs_lm6.registerTempTable("buypasrs_lm6")				  
	buyparts_lm6_schemardd.insertInto("buypasrs_lm6",overwrite=True)
	#3个月购买养护品标签
	buyparts_lm3 = union_rdd.filter(lambda x: (today - datetime.datetime.strptime(x[1][0],"%Y-%m-%d %H:%M:%S.0")).days<=90) \
					.aggregateByKey((0,0,0),lambda x,y: (x[0]+1,x[1]+float(y[1]),x[2]+(float(y[2]))),lambda x,y:(x[0]+y[0],x[1]+y[1],x[2]+y[2])).map(lambda x: (x[0],int(x[1][0]),int(x[1][1]),int(x[1][2])))
	buyparts_lm3_schemardd = sqlContext.applySchema(buyparts_lm3,buyparts_lm3_schema).repartition(50)
	buypasrs_lm3 = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM3)
	buypasrs_lm3.registerTempTable("buypasrs_lm3")				  
	buyparts_lm3_schemardd.insertInto("buypasrs_lm3",overwrite=True)

if __name__ == '__main__':
	getBuyPartsTag(sc,sqlContext)