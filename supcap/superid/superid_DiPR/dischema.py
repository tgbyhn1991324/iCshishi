try:
	from pyspark.sql.types import *
except:
	from pyspark.sql import *

__all__ = (
	'diphoneweibo_rdd_schema',
	'diridweibo_rdd_schema',
	'divinweibo_rdd_schema',
	'diphoneemail_rdd_schema',
	'divinemail_rdd_schema',
	'diridemail_rdd_schema',
	'dilicenseemail_rdd_schema',
	'diridweixin_rdd_schema',
	'dilicenseweixin_rdd_schema',
	'divinweixin_rdd_schema',
	'diphoneweixin_rdd_schema',
	'divinrid_rdd_schema',
	'diphonerid_rdd_schema',
	'dilicenserid_rdd_schema',
	'divinphone_rdd_schema',
	'divinlicense_rdd_schema',
	'diphonelicense_rdd_schema',
	)



diphoneweibo_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diridweibo_rdd_schema = StructType([
					StructField("RID",                          StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinweibo_rdd_schema = StructType([
					StructField("VIN",                          StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diphoneemail_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinemail_rdd_schema = StructType([
					StructField("VIN",                          StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diridemail_rdd_schema = StructType([
					StructField("RID",                          StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

dilicenseemail_rdd_schema = StructType([
					StructField("LICENSE",                      StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diphoneweixin_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("WEIXIN",                       StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diridweixin_rdd_schema = StructType([
					StructField("RID",                    	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinweixin_rdd_schema = StructType([
					StructField("VIN",                    	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

dilicenseweixin_rdd_schema = StructType([
					StructField("LICENSE",                	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinrid_rdd_schema =  StructType([
					StructField("VIN",                	         StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diphonerid_rdd_schema =  StructType([
					StructField("PHONE_NO",                	     StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

dilicenserid_rdd_schema =  StructType([
					StructField("LICENSE",                	     StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinphone_rdd_schema =  StructType([
					StructField("VIN",                	     	 StringType(), True),
					StructField("PHONE_NO",                      StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

divinlicense_rdd_schema =  StructType([
					StructField("VIN",                	     	 StringType(), True),
					StructField("LICENSE",                       StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])

diphonelicense_rdd_schema =  StructType([
					StructField("PHONE_NO",                	  	 StringType(), True),
					StructField("LICENSE",                       StringType(), True),
					StructField("FDi",                           FloatType(), True),
					StructField("Di",                           FloatType(),  True)
					])