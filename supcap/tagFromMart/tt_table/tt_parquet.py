from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *

conf = SparkConf().setAppName("PySpark_SUPCAP_TT_tag_init")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tt_table.tt_schema import *
from mathartsys.supcap.tagFromMart.tt_table.table_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(tt_mzmid_schema, HDFS_PATH_TT_MZMID)
	except Exception, e:
		print "initParquetFile fail1", str(e)
	try:
		initParquetFile(tt_table_schema, HDFS_PATH_TT_TABLE)
	except Exception, e:
		print "initParquetFile fail2", str(e)
	# try:
	# 	initParquetFile(tt_table_schema, "/project/supcap/iinsight/tagFromMart/tt_table/tt_final_table_sample")
	# except Exception, e:
	# 	print "initParquetFile fail3", str(e)