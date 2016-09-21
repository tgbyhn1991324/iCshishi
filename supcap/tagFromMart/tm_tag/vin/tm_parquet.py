from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *

conf = SparkConf().setAppName("PySpark_SUPCAP_TM_tag_init")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tm_tag.vin.tm_schema import *
from mathartsys.supcap.tagFromMart.tm_tag.vin.tm_tablepath import *

def initParquetFile(schema, path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

if __name__ == '__main__':
	try:
		initParquetFile(vin_tag_schema, HDFS_PATH_VIN_TAG)
	except Exception, e:
		print "initParquetFile fail1", str(e)