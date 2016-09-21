from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_KEYKEY_init")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.key_key_table.key_schema import *
from mathartsys.supcap.superid.key_key_table.key_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(idm_temptable_schema, HDFS_PATH_IDM_TEMP_TABLE)
	except Exception, e:
		print "initParquetFile fail0", str(e)
	try:
		initParquetFile(repair_order_temptable_schema, HDFS_PATH_REPAIR_ORDER_TEMP_TABLE)
	except Exception, e:
		print "initParquetFile fail00", str(e)
	try:
		initParquetFile(upload_track_temptable_schema, HDFS_PATH_UPLOAD_TRACK_TEMP_TABLE)
	except Exception, e:
		print "initParquetFile fail00", str(e)
	try:
		initParquetFile(phonecookie_rdd_schema, HDFS_PATH_TT_PHONE_MZCOOKIE)
	except Exception, e:
		print "initParquetFile fail1", str(e)
	try:
		initParquetFile(phoneweibo_rdd_schema, HDFS_PATH_TT_PHONE_WEIBO)
	except Exception, e:
		print "initParquetFile fail2", str(e)
	try:
		initParquetFile(ridweibo_rdd_schema, HDFS_PATH_TT_RID_WEIBO)
	except Exception, e:
		print "initParquetFile fail3", str(e)		
	try:
		initParquetFile(vinweibo_rdd_schema, HDFS_PATH_TT_VIN_WEIBO)
	except Exception, e:
		print "initParquetFile fail4", str(e)
	try:
		initParquetFile(phoneemail_rdd_schema, HDFS_PATH_TT_PHONE_EMAIL)
	except Exception, e:
		print "initParquetFile fail5", str(e)
	try:
		initParquetFile(vinemail_rdd_schema, HDFS_PATH_TT_VIN_EMAIL)
	except Exception, e:
		print "initParquetFile fail6", str(e)
	try:
		initParquetFile(ridemail_rdd_schema, HDFS_PATH_TT_RID_EMAIL)
	except Exception, e:
		print "initParquetFile fail7", str(e)
	try:
		initParquetFile(licenseemail_rdd_schema, HDFS_PATH_TT_LICENSE_EMAIL)
	except Exception, e:
		print "initParquetFile fail8", str(e)
	try:
		initParquetFile(phoneweixin_rdd_schema, HDFS_PATH_TT_PHONE_WEIXIN)
	except Exception, e:
		print "initParquetFile fail9", str(e)
	try:
		initParquetFile(ridweixin_rdd_schema, HDFS_PATH_TT_RID_WEIXIN)
	except Exception, e:
		print "initParquetFile fail10", str(e)
	try:
		initParquetFile(vinweixin_rdd_schema, HDFS_PATH_TT_VIN_WEIXIN)
	except Exception, e:
		print "initParquetFile fail11", str(e)
	try:
		initParquetFile(licenseweixin_rdd_schema, HDFS_PATH_TT_LICENSE_WEIXIN)
	except Exception, e:
		print "initParquetFile fail12", str(e)
	try:
		initParquetFile(vinrid_rdd_schema, HDFS_PATH_TT_VIN_RID)
	except Exception, e:
		print "initParquetFile fail13", str(e)
	try:
		initParquetFile(phonerid_rdd_schema, HDFS_PATH_TT_PHONE_RID)
	except Exception, e:
		print "initParquetFile fail14", str(e)
	try:
		initParquetFile(licenserid_rdd_schema, HDFS_PATH_TT_LICENSE_RID)
	except Exception, e:
		print "initParquetFile fail15", str(e)
	try:
		initParquetFile(vinphone_rdd_schema, HDFS_PATH_TT_VIN_PHONE)
	except Exception, e:
		print "initParquetFile fail16", str(e)
	try:
		initParquetFile(vinlicense_rdd_schema, HDFS_PATH_TT_VIN_LICENSE)
	except Exception, e:
		print "initParquetFile fail17", str(e)
	try:
		initParquetFile(phonelicense_rdd_schema, HDFS_PATH_TT_PHONE_LICENSE)
	except Exception, e:
		print "initParquetFile fail18", str(e)
