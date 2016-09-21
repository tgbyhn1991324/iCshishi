# -*- coding: utf-8 -*-
import sys

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_TT_Table")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tt_table.table_tablepath import *
from mathartsys.supcap.tagFromMart.tt_table.tt_schema import *
from mathartsys.supcap.tagFromMart.tt_table.tt_map import *


def uniontag(dict1,dict2):
	ret = {}
	dict1["tag_id"] = dict1.get("tag_id",None)
	dict2["tag_id"] = dict2.get("tag_id",None)
	for key in dict1:
		if dict1.get(key) and dict2.get(key):
			if key[1:3] == '00':
				ret[key] = dict1[key] + dict2[key]
			elif key[1:3] == '02':
				ret[key] = max(dict1[key],dict2[key])
			elif key[1:3] == '03':
				ret[key] = min(dict1[key],dict2[key])
			elif key[1:3] == '05':
				ret[key] = (dict1[key] + dict2[key])
			elif key[1:3] == '06':
				ret [key] = dict1[key]	
			elif key[1:3] == '07':
				ret[key] = 2L
		elif dict1.get(key):
			ret[key] = dict1[key]
		elif dict2.get(key):
			ret[key] = dict2[key]
		else:
			ret[key] = None
	return ret

def unionDict(dict1,dict2):
	if dict1 and dict2:
		dict1.update(**dict2)
		return dict1
	elif dict1:
		return dict1
	elif dict2:
		return dict2

def md5(str):
	import hashlib
	import types
	try:
		m = hashlib.md5()   
		m.update(str)
		return m.hexdigest()
	except Exception, e:
		return None

def addRid(row,rid):
	try:
		row["RID"] = md5(rid)
	except Exception, e:
		pass
	return row

def unionVinToDict(dict1,dict2,vin):
	if dict1 and dict2:
		dict1.update(**dict2)
		dict1["VIN"] = md5(vin)
		return dict1
	elif dict1:
		dict1["VIN"] = md5(vin)
		return dict1
	elif dict2:
		dict2["VIN"] = md5(vin)
		return dict2

def formatRowFromSchemawithMap(dictRow, schema,map):
	ret = []
	for field in schema.fields:
		if dictRow.has_key(map[field.name]):
			if isinstance(dictRow[map[field.name]],int) or isinstance(dictRow[map[field.name]],long):
				ret.append(int(dictRow[map[field.name]]))
			else:
				ret.append(dictRow[map[field.name]])
		else:
			ret.append(None)
	return ret

def getTTtable(sc,sqlContext):
	# cookie_tag_withphone = sqlContext.parquetFile(HDFS_PATH_TT_MZMID).map(lambda x: (x.PHONE_NO,x.asDict())).reduceByKey(uniontag)
	phone_tag = sqlContext.parquetFile(HDFS_PATH_PHONE_TAG).map(lambda x: (x.tag_id,x.asDict()))
	rid_tag = sqlContext.parquetFile(HDFS_PATH_RID_TAG).map(lambda x: (x.tag_id,x.asDict()))
	vin_tag = sqlContext.parquetFile(HDFS_PATH_VIN_TAG).map(lambda x: (x.tag_id,x.asDict()))
	rid_phone = sqlContext.parquetFile(HDFS_PATH_TT_PHONE_RID).map(lambda x: (x.PHONE_NO,x.RID)).distinct()
	vin_rid = sqlContext.parquetFile(HDFS_PATH_TT_VIN_RID).filter(lambda x: x.SYSTEM == 'CEM').map(lambda x: (x.RID,x.VIN))
	phone_cookie_tag = phone_tag
	# .fullOuterJoin(cookie_tag_withphone).map(lambda x: (x[0],unionDict(x[1][0],x[1][1])))
	phonecookie_tag_withrid = rid_phone.join(phone_cookie_tag).map(lambda x: (x[1][0],x[1][1])).reduceByKey(uniontag)
	rid_phone_cookie_tag = rid_tag.fullOuterJoin(phonecookie_tag_withrid).map(lambda x: (x[0],unionDict(x[1][0],x[1][1])))
	rid_phone_cookie_tag_withvin = vin_rid.leftOuterJoin(rid_phone_cookie_tag).map(lambda x: (x[1][0],addRid(x[1][1],x[0])))
	vin_rid_phone_cookie_tag = vin_tag.leftOuterJoin(rid_phone_cookie_tag_withvin).map(lambda x: unionVinToDict(x[1][0],x[1][1],x[0])).map(lambda x: formatRowFromSchemawithMap(x,tt_table_schema,ti_tt_map))
	vin_rid_phone_cookie_tag_schemardd = sqlContext.applySchema(vin_rid_phone_cookie_tag,tt_table_schema).repartition(50)
	tttable = sqlContext.parquetFile(HDFS_PATH_TT_TABLE)
	tttable.registerTempTable("tttable")				  
	vin_rid_phone_cookie_tag_schemardd.insertInto("tttable",overwrite=True)

if __name__ == '__main__':
	getTTtable(sc,sqlContext)