import sys

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_Mongo")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tt_table.table_tablepath import *
from mathartsys.supcap.tagFromMart.tt_table.tt_map import *

#HDFS_PATH_TT_TABLE = "/project/supcap/iinsight/tt_final_table_sample2)"
# table = sqlContext.parquetFile(HDFS_PATH_TT_TABLE).sample(False.0.033)
# table = sqlContext.parquetFile(HDFS_PATH_TT_TABLE)

def f(iterator):
	import pymongo
	import datetime
	today = datetime.datetime.today()
	# client = pymongo.MongoClient('10.203.98.7',port=27017)
	client = pymongo.MongoClient('10.203.44.184',port=27019)
	# db = client.test
	db = client.supcap
	db.authenticate('supcap','s1u2p3')
	for x in iterator:	
		hasr3 = False
		hasc3 = False
		hass3 = False
		hasca1 = False
		hasmz = False
		y = {}
		z = x.c()
		for key in z.keys():
			if z[key]: 
				y[tt_db_map[key]] = z[key]
		for key in digitlist:
			if y.get(key) == 0:
				del y[key]
		for key in repair_3m:
			if y.has_key(key):
				y["ZA"] = 1
				hasr3 = True
				break
		for key in repair_6m:
			if y.has_key(key) or hasr3:
				y["ZB"] = 1
				break
		for key in repair_12m:
			if y.has_key(key) or hasr3:
				y["ZC"] = 1
				break
		for key in claim_3m:
			if y.has_key(key):
				y["ZD"] = 1
				hasc3 = True
				break
		for key in claim_6m:
			if y.has_key(key) or hasc3:
				y["ZE"] = 1
				break
		for key in claim_12m:
			if y.has_key(key) or hasc3:
				y["ZF"] = 1
				break
		for key in store_3m:
			if y.has_key(key):
				y["ZG"] = 1
				hass3 = True
				break
		for key in store_6m:
			if y.has_key(key) or hass3:
				y["ZH"] = 1
				break
		for key in store_12m:
			if y.has_key(key) or hass3:
				y["ZI"] = 1
				break
		for key in call_1w:
			if y.has_key(key):
				y["ZJ"] = 1
				hasca1 = True
				break
		for key in call_1m:
			if y.has_key(key) or hasca1:
				y["ZK"] = 1
				break
		for key in call_3m:
			if y.has_key(key) or hasca1:
				y["ZL"] = 1
				break
		for key in call_tl:
			if y.has_key(key) or hasca1:
				y["ZM"] = 1
				break
		for key in mz_1m:
			if y.has_key(key):
				y["ZN"] = 1
				hasmz = True
				break
		for key in mz_2m:
			if y.has_key(key) or hasmz:
				y["ZO"] = 1
				break
		for key in mz_3m:
			if y.has_key(key) or hasmz:
				y["ZP"] = 1
				break
		if y.get('AS') == 2:
			y['AB'] = 999
			y['NE'] = 999
			y['AC'] = 999
			y['AD'] = 999
			y['AE'] = 999
			y['AF'] = 999
			y['AG'] = 999
			y['AH'] = 999
			y['AI'] = 999
			y['AJ'] = 999
			y['AK'] = 999
			y['AL'] = 999
			y['AM'] = 999
		for i in nonelist:
			if y.get(i) == -1:
				del y[i]
		if len(y.keys()) > 1:
			# db.test.insert_one(y)
			# db.iinsight.save(y)
			y['version'] = '1.1.1'
			db.iinsight.replace_one({"_id":y["_id"]},y,upsert=True)
	db.iinsight_updatetime.replace_one({"_id":"updatetime"},{"_id":"updatetime","lastupdatetime":today},upsert=True)


def tt_mongo(sc,sqlContext):
	table = sqlContext.parquetFile(HDFS_PATH_TT_TABLE)
	table.foreachPartition(f)

if __name__ == '__main__':
	tt_mongo(sc,sqlContext)