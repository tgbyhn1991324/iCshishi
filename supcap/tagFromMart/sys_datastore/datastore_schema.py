try:
	from pyspark.sql.types import *
except:
	from pyspark.sql import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_tags import *

__all__ = (
	'tt_asc_repair_order_schema', 
	'tt_asc_balance_order_schema',
	'tt_asc_claim_order_schema',
	'tt_asc_bo_repair_part_schema',
	'tt_asc_bo_sale_part_schema',
	'datastore_tag_LM3_schema',
	'datastore_tag_LM6_schema',
	'datastore_tag_LM12_schema',
	'tt_asc_repair_order_all_time_schema',
	'tt_asc_balance_order_alltime_schema',
	'datastore_tag_lately_schema',
	'buyparts_lm3_schema',
	'buyparts_lm6_schema',
	'buyparts_lm12_schema',
	'claim_tag_rdd_schema',
	'datastore_tag_1YB_schema'
	)


tt_asc_repair_order_schema = StructType([
					StructField("RO_ASC_CODE",                   StringType(), True),
					StructField("RO_RO_NO",                      StringType(), True),
					StructField("RO_VIN",                        StringType(), True),
					StructField("RO_BRAND",                      StringType(), True),
					StructField("RO_START_TIME",                 StringType(), True),
					StructField("RO_BALANCE_TIME",               StringType(), True),
					StructField("RO_IN_MILEAGE",                 StringType(), True),
					StructField("RO_OUT_MILEAGE",                StringType(), True),
					StructField("RO_DT",                         IntegerType(),   True)
					])

tt_asc_balance_order_schema = StructType([
					StructField("BO_ASC_CODE",                   StringType(), True),
					StructField("BO_B_BALANCE_NO",               StringType(), True),
					StructField("BO_VIN",                        StringType(), True),
					StructField("BO_REPAIR_TYPE_DESC",           StringType(), True),
					StructField("BO_TOTAL_AMOUNT",               StringType(), True),
					StructField("BO_BALANCE_TIME",               StringType(), True),
					StructField("BO_RO_NO",           		     StringType(), True),
					StructField("BO_DT",                         IntegerType(),   True)
					])

tt_asc_claim_order_schema = StructType([
					StructField("CO_ASC_CODE",                     StringType(), True),
					StructField("CO_CLAIM_NO_ID",                  StringType(), True),
					StructField("CO_RO_NO",                      StringType(), True),
					StructField("CO_VIN",      					   StringType(), True),
					StructField("CO_CLAIM_TYPE",                   StringType(), True),
					StructField("CO_GROSS_CREDIT",          	   StringType(), True),
					StructField("CO_RO_OPEN_DATE",                 StringType(), True),
					StructField("CO_DT",                           IntegerType(),   True)
					])

tt_asc_bo_repair_part_schema = StructType([
					StructField("RP_REPAIR_PART_ID",              StringType(), True),
					StructField("RP_PART_NO",                     StringType(), True),
					StructField("RP_ASC_CODE",                    StringType(), True),
					StructField("RP_RO_NO",          		      StringType(), True),
					StructField("RP_BALANCE_NO",               	  StringType(), True),
					StructField("RP_PART_QUANTITY",               StringType(), True),
					StructField("RP_PART_SALE_AMOUNT",            StringType(), True),
					StructField("RP_DT",                          IntegerType(),   True)
					])

tt_asc_bo_sale_part_schema = StructType([
					StructField("SP_SALE_PART_ID",                StringType(), True),
					StructField("SP_ASC_CODE",                    StringType(), True),
					StructField("SP_RO_NO",                       StringType(), True),
					StructField("SP_BALANCE_NO",          		  StringType(), True),
					StructField("SP_PART_QUANTITY",               StringType(), True),
					StructField("SP_PART_SALE_AMOUNT",            StringType(), True),
					StructField("SP_DT",                          IntegerType(),   True)
					])

tt_asc_repair_order_all_time_schema = StructType([
					StructField("VIN",                             StringType(),True),
					StructField("START_TIME",                      StringType(),True),
					StructField("DT",                              IntegerType(),True)
					])

tt_asc_balance_order_alltime_schema = StructType([
					StructField("ASC_CODE",                         StringType(),True),
					StructField("B_BALANCE_NO",                     StringType(),True),
					StructField("RO_NO",                      		StringType(),True),
					StructField("DT",                               IntegerType(),True)
					])


datastore_tag_lately_schema = StructType([
					StructField("tag_id",              			 StringType(), True),
					StructField(VIN_DATE_LATELY,                 IntegerType(),   True),
					StructField(VIN_DAYS_LATELY,					 IntegerType(),   True)
					])

datastore_tag_LM3_schema = StructType([
					StructField("tag_id",              			StringType(), True),
					StructField(VIN_TIMES_INASC_TOTAL_LM3,		  IntegerType(), True),
					StructField(VIN_MILE_IO_GAP_MAX_LM3,  		  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MAX_LM3, 	  	  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MIN_LM3,  		  IntegerType(), True),
					StructField(VIN_DAY_INASC_TOTAL_LM3, 		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_TOTAL_LM3,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_TOTAL_LM3,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_MAX_LM3,    		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_MAX_LM3,		  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_MAX_LM3,		  IntegerType(), True),
					StructField(VIN_TIMES_ACC_ASC_LM3,    		  IntegerType(), True),
					StructField(VIN_TIMES_ACCIDENT_REPAIR_LM3,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCIDENT_REPAIR_LM3,	  IntegerType(), True),
					StructField(VIN_TIMES_NOMAL_REPAIR_LM3,  	  IntegerType(), True),
					StructField(VIN_TIMES_INR_NOMAL_REPAIR_LM3,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_NOMAL_REPAIR_LM3,	  IntegerType(), True),
					StructField(VIN_AMOUNT_NOMAL_REPAIR_LM3, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_NOMAL_REPAIR_LM3,	  IntegerType(), True),											
					StructField(VIN_AMOUNT_OUTR_NOMAL_REPAIR_LM3,  IntegerType(), True),
					StructField(VIN_TIMES_NFREE_NOMAL_REPAIR_LM3,  IntegerType(), True),
					StructField(VIN_AMOUNT_NFREE_NOMAL_REPAIR_LM3, IntegerType(), True),
					StructField(VIN_TIMES_FREE_NOMAL_REPAIR_LM3,	  IntegerType(), True),
					StructField(VIN_TIMES_MAINTENANCE_LM3,   	  IntegerType(), True),
					StructField(VIN_TIMES_INR_MAINTENANCE_LM3,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_MAINTENANCE_LM3,	  IntegerType(), True),
					StructField(VIN_AMOUNT_MAINTENANCE_LM3,	   	  IntegerType(), True),					
					StructField(VIN_AMOUNT_INR_MAINTENANCE_LM3, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_MAINTENANCE_LM3,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCOUNT_LM3,   	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_ACCOUNT_LM3,	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_ACCOUNT_LM3,	   	  IntegerType(), True)
					])

datastore_tag_LM6_schema = StructType([
					StructField("tag_id",              			StringType(), True),
					StructField(VIN_TIMES_INASC_TOTAL_LM6,		  IntegerType(), True),
					StructField(VIN_MILE_IO_GAP_MAX_LM6,  		  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MAX_LM6, 	  	  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MIN_LM6,  		  IntegerType(), True),
					StructField(VIN_DAY_INASC_TOTAL_LM6, 		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_TOTAL_LM6,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_TOTAL_LM6,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_MAX_LM6,    		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_MAX_LM6,		  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_MAX_LM6,		  IntegerType(), True),
					StructField(VIN_TIMES_ACC_ASC_LM6,    		  IntegerType(), True),
					StructField(VIN_TIMES_ACCIDENT_REPAIR_LM6,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCIDENT_REPAIR_LM6,	  IntegerType(), True),
					StructField(VIN_TIMES_NOMAL_REPAIR_LM6,  	  IntegerType(), True),
					StructField(VIN_TIMES_INR_NOMAL_REPAIR_LM6,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_NOMAL_REPAIR_LM6,	  IntegerType(), True),
					StructField(VIN_AMOUNT_NOMAL_REPAIR_LM6, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_NOMAL_REPAIR_LM6,	  IntegerType(), True),											
					StructField(VIN_AMOUNT_OUTR_NOMAL_REPAIR_LM6,  IntegerType(), True),
					StructField(VIN_TIMES_NFREE_NOMAL_REPAIR_LM6,  IntegerType(), True),
					StructField(VIN_AMOUNT_NFREE_NOMAL_REPAIR_LM6, IntegerType(), True),
					StructField(VIN_TIMES_FREE_NOMAL_REPAIR_LM6,	  IntegerType(), True),
					StructField(VIN_TIMES_MAINTENANCE_LM6,   	  IntegerType(), True),
					StructField(VIN_TIMES_INR_MAINTENANCE_LM6,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_MAINTENANCE_LM6,	  IntegerType(), True),
					StructField(VIN_AMOUNT_MAINTENANCE_LM6,	   	  IntegerType(), True),					
					StructField(VIN_AMOUNT_INR_MAINTENANCE_LM6, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_MAINTENANCE_LM6,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCOUNT_LM6,   	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_ACCOUNT_LM6,	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_ACCOUNT_LM6,	   	  IntegerType(), True)
					])

datastore_tag_LM12_schema = StructType([
					StructField("tag_id",              			StringType(), True),
					StructField(VIN_TIMES_INASC_TOTAL_LM12,		  IntegerType(), True),
					StructField(VIN_MILE_IO_GAP_MAX_LM12,  		  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MAX_LM12, 	  	  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MIN_LM12,  		  IntegerType(), True),
					StructField(VIN_DAY_INASC_TOTAL_LM12, 		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_TOTAL_LM12,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_TOTAL_LM12,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_MAX_LM12,    		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_MAX_LM12,		  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_MAX_LM12,		  IntegerType(), True),
					StructField(VIN_TIMES_ACC_ASC_LM12,    		  IntegerType(), True),
					StructField(VIN_TIMES_ACCIDENT_REPAIR_LM12,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCIDENT_REPAIR_LM12,	  IntegerType(), True),
					StructField(VIN_TIMES_NOMAL_REPAIR_LM12,  	  IntegerType(), True),
					StructField(VIN_TIMES_INR_NOMAL_REPAIR_LM12,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_NOMAL_REPAIR_LM12,	  IntegerType(), True),
					StructField(VIN_AMOUNT_NOMAL_REPAIR_LM12, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_NOMAL_REPAIR_LM12,	  IntegerType(), True),											
					StructField(VIN_AMOUNT_OUTR_NOMAL_REPAIR_LM12,  IntegerType(), True),
					StructField(VIN_TIMES_NFREE_NOMAL_REPAIR_LM12,  IntegerType(), True),
					StructField(VIN_AMOUNT_NFREE_NOMAL_REPAIR_LM12, IntegerType(), True),
					StructField(VIN_TIMES_FREE_NOMAL_REPAIR_LM12,	  IntegerType(), True),
					StructField(VIN_TIMES_MAINTENANCE_LM12,   	  IntegerType(), True),
					StructField(VIN_TIMES_INR_MAINTENANCE_LM12,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_MAINTENANCE_LM12,	  IntegerType(), True),
					StructField(VIN_AMOUNT_MAINTENANCE_LM12,	   	  IntegerType(), True),					
					StructField(VIN_AMOUNT_INR_MAINTENANCE_LM12, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_MAINTENANCE_LM12,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCOUNT_LM12,   	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_ACCOUNT_LM12,	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_ACCOUNT_LM12,	   	  IntegerType(), True)
					])

datastore_tag_1YB_schema = StructType([
					StructField("tag_id",              			StringType(), True),
					StructField(VIN_TIMES_INASC_TOTAL_1YB,		  IntegerType(), True),
					StructField(VIN_MILE_IO_GAP_MAX_1YB,  		  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MAX_1YB, 	  	  IntegerType(), True),
					StructField(VIN_MILE_I_GAP_MIN_1YB,  		  IntegerType(), True),
					StructField(VIN_DAY_INASC_TOTAL_1YB, 		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_TOTAL_1YB,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_TOTAL_1YB,	  IntegerType(), True),
					StructField(VIN_DAY_INASC_MAX_1YB,    		  IntegerType(), True),
					StructField(VIN_DAY_INASC_INR_MAX_1YB,		  IntegerType(), True),
					StructField(VIN_DAY_INASC_OUTR_MAX_1YB,		  IntegerType(), True),
					StructField(VIN_TIMES_ACC_ASC_1YB,    		  IntegerType(), True),
					StructField(VIN_TIMES_ACCIDENT_REPAIR_1YB,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCIDENT_REPAIR_1YB,	  IntegerType(), True),
					StructField(VIN_TIMES_NOMAL_REPAIR_1YB,  	  IntegerType(), True),
					StructField(VIN_TIMES_INR_NOMAL_REPAIR_1YB,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_NOMAL_REPAIR_1YB,	  IntegerType(), True),
					StructField(VIN_AMOUNT_NOMAL_REPAIR_1YB, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_NOMAL_REPAIR_1YB,	  IntegerType(), True),											
					StructField(VIN_AMOUNT_OUTR_NOMAL_REPAIR_1YB,  IntegerType(), True),
					StructField(VIN_TIMES_NFREE_NOMAL_REPAIR_1YB,  IntegerType(), True),
					StructField(VIN_AMOUNT_NFREE_NOMAL_REPAIR_1YB, IntegerType(), True),
					StructField(VIN_TIMES_FREE_NOMAL_REPAIR_1YB,	  IntegerType(), True),
					StructField(VIN_TIMES_MAINTENANCE_1YB,   	  IntegerType(), True),
					StructField(VIN_TIMES_INR_MAINTENANCE_1YB,	  IntegerType(), True),
					StructField(VIN_TIMES_OUTR_MAINTENANCE_1YB,	  IntegerType(), True),
					StructField(VIN_AMOUNT_MAINTENANCE_1YB,	   	  IntegerType(), True),					
					StructField(VIN_AMOUNT_INR_MAINTENANCE_1YB, 	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_MAINTENANCE_1YB,	  IntegerType(), True),
					StructField(VIN_AMOUNT_ACCOUNT_1YB,   	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_INR_ACCOUNT_1YB,	   	  IntegerType(), True),
					StructField(VIN_AMOUNT_OUTR_ACCOUNT_1YB,	   	  IntegerType(), True)
					])

buyparts_lm3_schema = StructType([
					StructField("tag_id",              		       StringType(), True),
					StructField(VIN_TIMES_BUY_RARTS_LM3,             IntegerType(), True),
					StructField(VIN_NUM_BUY_RARTS_LM3,               IntegerType(), True),
					StructField(VIN_AMOUNT_BUY_RARTS_LM3,            IntegerType(), True)
					])

buyparts_lm6_schema = StructType([
					StructField("tag_id",              		       StringType(), True),
					StructField(VIN_TIMES_BUY_RARTS_LM6,             IntegerType(), True),
					StructField(VIN_NUM_BUY_RARTS_LM6,               IntegerType(), True),
					StructField(VIN_AMOUNT_BUY_RARTS_LM6,            IntegerType(), True)
					])

buyparts_lm12_schema = StructType([
					StructField("tag_id",              		       StringType(), True),
					StructField(VIN_TIMES_BUY_RARTS_LM12,             IntegerType(), True),
					StructField(VIN_NUM_BUY_RARTS_LM12,               IntegerType(), True),
					StructField(VIN_AMOUNT_BUY_RARTS_LM12,            IntegerType(), True)
					])

claim_tag_rdd_schema = StructType([
					StructField("tag_id",              		           StringType(), True),
					StructField(VIN_DAYS_WAR_CLAIM_LATELY,               IntegerType(), True),
					StructField(VIN_DATE_WAR_CLAIM_LATELY,               IntegerType(), True),
					StructField(VIN_DAYS_NWAR_CLAIM_LATELY,              IntegerType(), True),
					StructField(VIN_DATE_NWAR_CLAIM_LATELY,              IntegerType(), True),
					StructField(VIN_TIMES_WAR_CLAIM_LM3,                 IntegerType(), True),
					StructField(VIN_AMOUNT_WAR_CLAIM_LM3,            	 IntegerType(), True),
					StructField(VIN_TIMES_GOODWILL_CLAIM_LM3,            IntegerType(), True),
					StructField(VIN_AMOUNT_GOODWILL_CLAIM_LM3,           IntegerType(), True),
					StructField(VIN_TIMES_WAR_CLAIM_LM6,                 IntegerType(), True),
					StructField(VIN_AMOUNT_WAR_CLAIM_LM6,            	 IntegerType(), True),
					StructField(VIN_TIMES_GOODWILL_CLAIM_LM6,            IntegerType(), True),
					StructField(VIN_AMOUNT_GOODWILL_CLAIM_LM6,           IntegerType(), True),
					StructField(VIN_TIMES_WAR_CLAIM_LM12,                IntegerType(), True),
					StructField(VIN_AMOUNT_WAR_CLAIM_LM12,               IntegerType(), True),
					StructField(VIN_TIMES_GOODWILL_CLAIM_LM12,           IntegerType(), True),
					StructField(VIN_AMOUNT_GOODWILL_CLAIM_LM12,          IntegerType(), True)				
					])


'''
def formatRowFromSchema(dictRow, schema):
	ret = []
	for field in schema.fields:
		if dictRow.has_key(field.name):
			ret.append(dictRow[field.name])
		else:
			ret.append(None)
	return ret
'''