import sys 
import time 
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
try:
	from pyspark.sql.types import *
except:
	from pyspark.sql import *

conf = SparkConf().setAppName("PySpark_SUPCAP_TableInit")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

__all__ = (
	'series_value_schema', 
	'initParquetFile'
	)

def initParquetFile(schema,path):
	empty_rdd = sc.parallelize([])
	srdd = sqlContext.applySchema(empty_rdd, schema)
	srdd.saveAsParquetFile(path)

series_value_schema = StructType([StructField("tag",  MapType(StringType(), IntegerType(),False), False)])

if __name__ == '__main__':
	try:
		initParquetFile(series_value_schema, '/project/supcap/iinsight/data_mart/lookup_table/tag_value')
	except Exception, e:
		print "initParquetFile fail" +str(e)