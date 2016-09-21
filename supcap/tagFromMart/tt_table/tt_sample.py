# -*- coding: utf-8 -*-
import sys

if __name__ == '__main__':
	sys.path.insert(0, "../../../")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("PySpark_SUPCAP_SAMPLE_Table")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.tt_table.table_tablepath import *
from mathartsys.supcap.tagFromMart.tt_table.tt_schema import *

def formatRowFromSchema(dictRow, schema):
	ret = []
	for field in schema.fields:
		if dictRow.has_key(field.name):
			if isinstance(dictRow[field.name],int) or isinstance(dictRow[field.name],long):
				ret.append(int(dictRow[field.name]))
			else:
				ret.append(dictRow[field.name])
		else:
			ret.append(None)
	return ret

table = sqlContext.parquetFile(HDFS_PATH_TT_TABLE).sample(False,0.01).map(lambda x: x.asDict()).map(lambda x: formatRowFromSchema(x,tt_table_schema))
table_schemardd = sqlContext.applySchema(table,tt_table_schema)
table_sample = sqlContext.parquetFile("/project/supcap/iinsight/tt_table/tt_final_table_sample3")
table_sample.registerTempTable("sampletable")
table_schemardd.insertInto("sampletable",overwrite=True)
#table_schemardd.saveAsParquetFile("/project/supcap/iinsight/tt_table/tt_final_table_sample2")