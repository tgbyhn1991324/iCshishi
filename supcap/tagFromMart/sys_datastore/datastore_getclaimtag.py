# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_CLAIM_TAG")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tablepath import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_schema import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_function import *
# from mathartsys.supcap.tagFromMart.sys_datastore.datastore_mapkey import *

def getClaimTag(sc,sqlContext):
	claim_pf = sqlContext.parquetFile(HDFS_PATH_DT_CLAIM_ORDER_INFO)
	#(u'LSGWL52D14S283540', (u'FO', u'269.1', u'2005-02-01 00:00:00.0')
	claim_tag_rdd = claim_pf.map(lambda x: (x.vin,(x.claim_type,x.gross_credit,x.ro_open_date))) \
							 .aggregateByKey(((999999,)*4+(0,)*12),seqFunciton,combFunction).map(lambda x: [x[0],]+format(x[1]))
	claim_tag_rdd_schemardd = sqlContext.applySchema(claim_tag_rdd,claim_tag_rdd_schema).coalesce(100)
	claim_tag = sqlContext.parquetFile(HDFS_PATH_VIN_DATASTORE_CLAIM_TAG)
	claim_tag.registerTempTable("claim_tag")				  
	claim_tag_rdd_schemardd.insertInto("claim_tag",overwrite=True)

if __name__ == '__main__':
	getClaimTag(sc,sqlContext)