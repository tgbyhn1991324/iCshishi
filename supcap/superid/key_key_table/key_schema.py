try:
	from pyspark.sql.types import *
except:
	from pyspark.sql import *

__all__ = (
	'idm_temptable_schema',
	'repair_order_temptable_schema',
	'phonecookie_rdd_schema', 
	'phoneweibo_rdd_schema',
	'ridweibo_rdd_schema',
	'vinweibo_rdd_schema',
	'phoneemail_rdd_schema',
	'vinemail_rdd_schema',
	'ridemail_rdd_schema',
	'licenseemail_rdd_schema',
	'ridweixin_rdd_schema',
	'licenseweixin_rdd_schema',
	'vinweixin_rdd_schema',
	'phoneweixin_rdd_schema',
	'vinrid_rdd_schema',
	'phonerid_rdd_schema',
	'licenserid_rdd_schema',
	'vinphone_rdd_schema',
	'vinlicense_rdd_schema',
	'phonelicense_rdd_schema',
	'upload_track_temptable_schema'
	)

idm_temptable_schema = StructType([
					StructField("RID",                     		StringType(), True),
					StructField("PHONE_NO",                     StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("LICENSE",                     StringType(), True),
					StructField("WEIXIN",                       StringType(), True),
					StructField("VIN1",                 	    StringType(), True),
					StructField("VIN2",                 	    StringType(), True),
					])

repair_order_temptable_schema = StructType([
					StructField("ASC_CODE",                     StringType(), True),
					StructField("RO_NO",                        StringType(), True),
					StructField("VIN",                          StringType(), True),
					StructField("LICENSE",                      StringType(), True),
					StructField("PNONE",                        StringType(), True),
					StructField("MOBILE",                 	    StringType(), True),
					StructField("TIME",                 	    StringType(), True),
					StructField("DT",                 	   		LongType(),   True),
					])

upload_track_temptable_schema =  StructType([
					StructField("VIN",                	  	 	StringType(), True),
					StructField("RID",                       	StringType(), True),
					StructField("PHONE1",                    	StringType(), True),
					StructField("PHONE2",                    	StringType(), True),
					StructField("PHONE3",                 	 	StringType(), True),
					StructField("PHONE4",                      	StringType(), True),
					StructField("EMAIL",                      	StringType(), True),
					StructField("TIME",                      	StringType(), True),					
					])

phonecookie_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("MZ_COOKIE",                    StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	LongType(),   True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

phoneweibo_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

ridweibo_rdd_schema = StructType([
					StructField("RID",                          StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinweibo_rdd_schema = StructType([
					StructField("VIN",                          StringType(), True),
					StructField("WEIBO",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

phoneemail_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinemail_rdd_schema = StructType([
					StructField("VIN",                          StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

ridemail_rdd_schema = StructType([
					StructField("RID",                          StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

licenseemail_rdd_schema = StructType([
					StructField("LICENSE",                      StringType(), True),
					StructField("EMAIL",                        StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

phoneweixin_rdd_schema = StructType([
					StructField("PHONE_NO",                     StringType(), True),
					StructField("WEIXIN",                       StringType(), True),
					StructField("SYSTEM",                       StringType(), True),
					StructField("TIMES",                        LongType(),   True),
					StructField("LASTDATE",                 	StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

ridweixin_rdd_schema = StructType([
					StructField("RID",                    	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinweixin_rdd_schema = StructType([
					StructField("VIN",                    	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

licenseweixin_rdd_schema = StructType([
					StructField("LICENSE",                	     StringType(), True),
					StructField("WEIXIN",                        StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinrid_rdd_schema =  StructType([
					StructField("VIN",                	         StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

phonerid_rdd_schema =  StructType([
					StructField("PHONE_NO",                	     StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

licenserid_rdd_schema =  StructType([
					StructField("LICENSE",                	     StringType(), True),
					StructField("RID",                           StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinphone_rdd_schema =  StructType([
					StructField("VIN",                	     	 StringType(), True),
					StructField("PHONE_NO",                      StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

vinlicense_rdd_schema =  StructType([
					StructField("VIN",                	     	 StringType(), True),
					StructField("LICENSE",                       StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

phonelicense_rdd_schema =  StructType([
					StructField("PHONE_NO",                	  	 StringType(), True),
					StructField("LICENSE",                       StringType(), True),
					StructField("SYSTEM",                        StringType(), True),
					StructField("TIMES",                         LongType(),   True),
					StructField("LASTDATE",                 	 StringType(), True),
					StructField("DEFAULTDi",                      FloatType(),  True)
					])

