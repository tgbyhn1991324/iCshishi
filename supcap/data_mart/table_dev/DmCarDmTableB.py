# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_vehicleTableBCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.table_list.dol_table import *
from mathartsys.supcap.data_mart.table_list.doss_table import *
from mathartsys.supcap.data_mart.table_list.cem_cx_asset_tt_table import *
from mathartsys.supcap.data_mart.table_list.cem_cx_be_validated_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import *
from mathartsys.supcap.data_mart.table_list.datastore_tt_asc_repair_order_table import *

def vehicleTableBGet(sc,sqlContext):
	#vehicle_info_b
	#品牌、购车日期、身份证号、最近里程、最近进站维修日期、是否会员、是否带Onstar
	#VIN = 80, RID = 74  (u'LSGVS54Z55Y014429', (u'320503196904262512',))
	cat_pf = sqlContext.parquetFile(HDFS_PATH_DT_CUSTOMER_INFO_CEM).map(lambda x: (x.vin,{'rid':x.rid}))
	dvia_pf = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_A).map(lambda x: (x.vin,{'brand_name':x.brand_name,'buy_date':x.buy_date})) #取全VIN号
	#getTempTagFromTt_asc_repair_order (datastore)
	taro_tf = sc.textFile(HDFS_PATH_DATASTORE_TT_ASC_REPAIR_ORDER_ALL)
	taro_valid_rdd = taro_tf.map(lambda x: x.split("\x01"))
	taro_rows = taro_valid_rdd.filter(lambda x: x[R_VIN] != u'null').map(lambda x: (x[R_VIN],(x[R_IN_MILEAGE],x[R_START_TIME]))) \
									.reduceByKey(getLatestMileage) \
									.map(lambda x: (x[0],{'mileage_latest':x[1][0],'start_date_latest':x[1][1]})).coalesce(300)
	vin_time_rows = dvia_pf.leftOuterJoin(cat_pf).map(lambda x: (x[0],joinDict(x[1]))) \
							.leftOuterJoin(taro_rows).map(lambda x: (x[0],joinDict(x[1]))).coalesce(100)

	#是否是会员 VIN_IF_MEMBERSHIP
	#etTempTagFromUser_attributes(idm)  [u'LSGLP83X5EF201725', u'\u96ea\u4f5b\u5170']
	ua_tf = sc.textFile(HDFS_PATH_IDM_USER_ATTRIBUTES)
	ua_valid_rdd = ua_tf.map(lambda x: x.split("\x01")).filter(lambda x: (x[1] in(u'1395304',u'5934170',u'5934168',u'5934169') and x[2] != 'null')) \
						.map(lambda x: (x[0],{x[1]:x[2]}))
	ua_mbs_rows = ua_valid_rdd.reduceByKey(listChgRow).map(lambda x: getMembershipTag(x[1])).filter(lambda x: x[0] != 0) \
								.reduceByKey(lambda x,y : min(x,y)).map(lambda x: (x[0],{'if_membership':x[1]})).coalesce(40)
	#是否二手车
	cbv_tf = sc.textFile(HDFS_PATH_CEM_CX_BE_VALIDATED)
	cbv_valid_rdd = cbv_tf.map(lambda x: x.split("\x01"))
	vin_UsedCar_rows = cbv_valid_rdd.filter(lambda x: x[SERIAL_NUM] != u'null').map(lambda x: (x[SERIAL_NUM],x[LOAD_FROM])) \
								.reduceByKey(getUsedCar).map(lambda x: (x[0],x[1])).map(lambda x: getUsedCarTag(x)) \
								.map(lambda x: (x[0],{'if_used_car':x[1]}))
	#是否带Onstar标签
	# tdpa_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PACKAGE_VALUE_DTL).map(lambda x: (x.src_package_id,x.has_onstar)).coalesce(20)
	# tdv_pf = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_VALUE_DTL).map(lambda x: (x.package_id,x.vin))
	# vin_Onstart_rows = tdv_pf.join(tdpa_pf).map(lambda x: getOnstarTag(x[1])) \
	# 							.map(lambda x: (x[0],{'if_onstar':x[1]}))  #(u'LSGDE53T38H147601', (2L,))
	tdpa_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PACKAGE_VALUE_DTL).map(lambda x: (x.src_package_id,x.has_onstar)).coalesce(20)
	tdv_pf = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_VALUE_DTL).map(lambda x: (x.package_id,x.vin))
	#(x[vin],(x[ifonstar],x[onstar_active])
	vin_cem_onstar = sc.textFile(HDFS_PATH_CEM_CX_ASSET_TT).map(lambda x: x.split('\x01')).map(lambda x: (x[80],(x[17],x[22]))) \
						.flatMapValues(lambda x: x)
	vin_Onstart_rows = tdv_pf.join(tdpa_pf).map(lambda x: getOnstarTag(x[1])) \
								.union(vin_cem_onstar).reduceByKey(lambda x,y: u'否' if (x in (u'否','N') and y in (u'否','N')) else u'是') \
								.map(lambda x: (x[0],{'if_onstar':x[1]}))  #(u'LSGDE53T38H147601', (2L,))	
	#vehicle_info_b
	try:
		car_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_B).map(lambda x: (x.vin,(x.asDict(),-1)))
	except:
		car_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	car_rows_b = vin_time_rows.leftOuterJoin(vin_UsedCar_rows).map(lambda x: (x[0],joinDict(x[1]))) \
							.leftOuterJoin(ua_mbs_rows).map(lambda x: (x[0],joinDict(x[1]))) \
							.union(vin_Onstart_rows).reduceByKey(combineDict) \
							.map(lambda x: (x[0],dict({'vin':x[0]},**x[1]))) \
							.map(lambda x: (x[0],(dict(x[1],**{'create_date':today_str,'update_date':today_str}),1))) \
							.union(car_temp_rdd).reduceByKey(lambda x,y:DeduplicationDict(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: x[1][0]) \
							.map(lambda x: formatRowFromSchema(x, dm_vehicle_info_b_schema)).coalesce(100)
	car_rows_b_schemardd = sqlContext.applySchema(car_rows_b, dm_vehicle_info_b_schema)
	car_b_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_B)
	car_b_schemardd.registerTempTable("dm_vehicle_info_b")
	car_rows_b_schemardd.insertInto("dm_vehicle_info_b",overwrite=True)

if __name__ == '__main__':
	vehicleTableBGet(sc,sqlContext)

