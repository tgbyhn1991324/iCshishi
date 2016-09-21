#filename:tag_schema.py
try:
	from pyspark.sql.types import *
except:
	from pyspark.sql import *

from mathartsys.supcap.superid.doss_phone_tags import *
from mathartsys.supcap.superid.phone_tags import * 

__all__ = (
	'phone_tag_schema',
	'formatRowFromSchema')

phone_tag_schema = StructType([
					StructField("tag_id",                              StringType(), False),
					StructField(PHONE_DAYS_INC_LATELY,                 IntegerType(), True),
					StructField(PHONE_DATE_INC_LATELY,                 IntegerType(), True),
					StructField(PHONE_TIMES_HARASS_LW1,                IntegerType(), True),
					StructField(PHONE_TIMES_SUGGESTION_LW1,            IntegerType(), True),
					StructField(PHONE_TIMES_RESUE_LW1,                 IntegerType(), True),
					StructField(PHONE_TIMES_COMPLAINT_LW1,             IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_LW1,               IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_TEST_DRIVING_LW1,  IntegerType(), True),
					StructField(PHONE_TIMES_INC_TOTAL_LW1,             IntegerType(), True),
					StructField(PHONE_TIMES_HARASS_LM1,                IntegerType(), True),
					StructField(PHONE_TIMES_SUGGESTION_LM1,            IntegerType(), True),
					StructField(PHONE_TIMES_RESUE_LM1,                 IntegerType(), True),
					StructField(PHONE_TIMES_COMPLAINT_LM1,             IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_LM1,               IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_TEST_DRIVING_LM1,  IntegerType(), True),
					StructField(PHONE_TIMES_INC_TOTAL_LM1,             IntegerType(), True),
					StructField(PHONE_TIMES_HARASS_LM3,                IntegerType(), True),
					StructField(PHONE_TIMES_SUGGESTION_LM3,            IntegerType(), True),
					StructField(PHONE_TIMES_RESUE_LM3,                 IntegerType(), True),
					StructField(PHONE_TIMES_COMPLAINT_LM3,             IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_LM3,               IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_TEST_DRIVING_LM3,  IntegerType(), True),
					StructField(PHONE_TIMES_INC_TOTAL_LM3,             IntegerType(), True),
					StructField(PHONE_TIMES_HARASS_LT,                 IntegerType(), True),
					StructField(PHONE_TIMES_SUGGESTION_LT,             IntegerType(), True),
					StructField(PHONE_TIMES_RESUE_LT,                  IntegerType(), True),
					StructField(PHONE_TIMES_COMPLAINT_LT,              IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_LT,                IntegerType(), True),
					StructField(PHONE_TIMES_CONSULT_TEST_DRIVING_LT,   IntegerType(), True),
					StructField(PHONE_TIMES_INC_TOTAL_LT,              IntegerType(), True),
					StructField(PHONE_TIMES_INDEALER_LM3,              IntegerType(), True),
					StructField(PHONE_TIMES_INDEALER_LM6,              IntegerType(), True),  
					StructField(PHONE_TIMES_INDEALER_LM12,             IntegerType(), True), 
					StructField(PHONE_DURATION_INDEALER_LM3,           IntegerType(), True), 
					StructField(PHONE_DURATION_INDEALER_LM6,           IntegerType(), True),
					StructField(PHONE_DURATION_INDEALER_LM12,          IntegerType(), True), 
					StructField(PHONE_DATE_INDEALER_LATELY,            IntegerType(), True),
					StructField(PHONE_DAYS_INDEALER_LATELY,            IntegerType(), True),
					StructField(PHONE_TIMES_CALL_LM3,                  IntegerType(), True),
					StructField(PHONE_TIMES_CALL_LM6,                  IntegerType(), True),
					StructField(PHONE_TIMES_CALL_LM12,                 IntegerType(), True),
					StructField(PHONE_DURATION_CALL_LM3,               IntegerType(), True),
					StructField(PHONE_DURATION_CALL_LM6,               IntegerType(), True),
					StructField(PHONE_DURATION_CALL_LM12,              IntegerType(), True),
					StructField(PHONE_DATE_CALL_LATELY,                IntegerType(), True),
					StructField(PHONE_DAYS_CALL_LATELY,                IntegerType(), True),
					StructField(PHONE_TIMES_MARKETING_ACTIVITY,        IntegerType(), True),
					StructField(PHONE_TIMES_TEST_DRIVING_LM3,          IntegerType(), True),
					StructField(PHONE_TIMES_TEST_DRIVING_LM6,          IntegerType(), True),
					StructField(PHONE_TIMES_TEST_DRIVING_LM12,         IntegerType(), True),
					StructField(PHONE_MILES_TEST_DRIVING_LM3,          IntegerType(), True),
					StructField(PHONE_MILES_TEST_DRIVING_LM6,          IntegerType(), True),
					StructField(PHONE_MILES_TEST_DRIVING_LM12,         IntegerType(), True),
					StructField(PHONE_DATE_TEST_DRIVING_LATELY,        IntegerType(), True),
					StructField(PHONE_DAYS_TEST_DRIVING_LATELY,        IntegerType(), True) 
					]) 

def formatRowFromSchema(dictRow, schema):
	ret = []
	for field in schema.fields:
		if dictRow.has_key(field.name):
			ret.append(dictRow[field.name])
		else:
			ret.append(None)
	return ret
