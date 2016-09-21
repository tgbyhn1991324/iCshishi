from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_IDASHBOARD_init")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_schema import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(datastore_tag_LM3_schema,HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM3)
	except Exception, e:
		print "initParquetFile fail3", str(e)

	try:
		initParquetFile(datastore_tag_LM6_schema,HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM6)
	except Exception, e:
		print "initParquetFile fail6", str(e)

	try:
		initParquetFile(datastore_tag_LM12_schema,HDFS_PATH_VIN_DATASTORE_MAIN_TAG_LM12)
	except Exception, e:
		print "initParquetFile fail12", str(e)

	try:
		initParquetFile(datastore_tag_1YB_schema,HDFS_PATH_VIN_DATASTORE_MAIN_TAG_1YB)
	except Exception, e:
		print "initParquetFile fail1yb", str(e)

	try:
		initParquetFile(buyparts_lm3_schema,HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM3)
	except Exception, e:
		print "initParquetFile fail3a", str(e)

	try:
		initParquetFile(buyparts_lm6_schema,HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM6)
	except Exception, e:
		print "initParquetFile fail6a", str(e)

	try:
		initParquetFile(buyparts_lm12_schema,HDFS_PATH_VIN_DATASTORE_MAIN_BUYPARTS_LM12)
	except Exception, e:
		print "initParquetFile fail12a", str(e)
		
	try:
		initParquetFile(datastore_tag_lately_schema,HDFS_PATH_VIN_DATASTORE_LATELY)
	except Exception, e:
		print "initParquetFile fail10", str(e)

	try:
		initParquetFile(claim_tag_rdd_schema,HDFS_PATH_VIN_DATASTORE_CLAIM_TAG)
	except Exception, e:
		print "initParquetFile fail11", str(e)



