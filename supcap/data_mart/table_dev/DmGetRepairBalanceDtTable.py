# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_iinsightDatastoreTable")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.datastore_mapkey import *
from mathartsys.supcap.data_mart.table_list.datastore_table import *
from mathartsys.supcap.data_mart.table_dev.DmFunctionDefine import DeduplicationDate

#将表DM_BALANCE_ORDER_INFO根据key值（asc_code，ro_no，vin)join表DM_REPAIR_ORDER_INFO，
#再根据vin号join表DM_VEHICLE_INFO_A生成DT_REPAIR_ORDER_TOTAL
def getRepairBalanceTable(sc,sqlContext):
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		dtRepairorderTotal_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL).map(lambda x: (x[0:3],x[3:]))
	except:
		dtRepairorderTotal_temp_rdd = sc.parallelize([])
	tt_asc_balance_order = sqlContext.parquetFile(HDFS_PATH_DM_BALANCE_ORDER_INFO)
	tt_asc_balance_order.registerTempTable("balance_order")	
	tt_asc_repair_order = sqlContext.parquetFile(HDFS_PATH_DM_REPAIR_ORDER_INFO)
	tt_asc_repair_order.registerTempTable("repair_order")	
	vehicle_info = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_A)
	vehicle_info.registerTempTable("vehicle_info")
	repair_balance_order = sqlContext.sql("select a.asc_code,a.ro_no,a.vin,a.balance_no,a.plant_no,a.balance_mode_code,a.balance_mode_name,a.license,a.engine_no,a.payment_type,a.pay_name,a.labour_amount,a.labour_amount_discount,a.repair_part_amount,a.repair_part_discount,a.sale_part_amount,a.sale_part_discount,a.add_item_amount,a.add_item_discount,a.over_item_amount,a.over_item_discount,a.net_amount,a.tax,a.total_amount,a.zero_balanced,a.receive_amount,a.repair_part_cost,a.sale_part_cost,a.derate_amount,a.payoff_tag,a.is_visiable,a.sgm_wrt_tag,a.repair_campgn_tag,a.is_wrt_closed,a.sgm_vin_tag,a.campaign_warranty_tag,a.balance_time,a.square_date,a.production_value,a.labour_cost,a.invoice_no_pay,a.repair_type,a.ro_type,a.discount_mode_code,a.repair_type_desc,a.ro_type_desc,b.start_time,b.in_mileage,b.out_mileage,b.deliverer_phone,b.deliverer_mobile,b.delivery_tag,b.delivery_date,b.is_change_mileage  \
							from balance_order a join repair_order b on a.asc_code = b.asc_code and a.ro_no = b.ro_no and a.vin = b.vin") 
	repair_balance_order.registerTempTable("repair_balance")
	repair_balance_vehicle = sqlContext.sql("select a.asc_code,a.ro_no,a.vin,a.balance_no,a.plant_no,a.balance_mode_code,a.balance_mode_name,a.license,a.engine_no,a.payment_type,a.pay_name,a.labour_amount,a.labour_amount_discount,a.repair_part_amount,a.repair_part_discount,a.sale_part_amount,a.sale_part_discount,a.add_item_amount,a.add_item_discount,a.over_item_amount,a.over_item_discount,a.net_amount,a.tax,a.total_amount,a.zero_balanced,a.receive_amount,a.repair_part_cost,a.sale_part_cost,a.derate_amount,a.payoff_tag,a.is_visiable,a.sgm_wrt_tag,a.repair_campgn_tag,a.is_wrt_closed,a.sgm_vin_tag,a.campaign_warranty_tag,a.balance_time,a.square_date,a.production_value,a.labour_cost,a.invoice_no_pay,a.repair_type,a.ro_type,a.discount_mode_code,a.repair_type_desc,a.ro_type_desc,a.start_time,a.in_mileage,a.out_mileage,a.deliverer_phone,a.deliverer_mobile,a.delivery_tag,a.delivery_date,a.is_change_mileage,  \
								b.brand_name,b.buy_date,b.car_model_name,b.car_type_name,b.car_color_name from repair_balance a join vehicle_info b on a.vin = b.vin")
	repair_balance_vehicle_rdd = repair_balance_vehicle.map(lambda x: (x[0:3],x[3:]+(today_str,today_str))) \
						.union(dtRepairorderTotal_temp_rdd).coalesce(2000) \
						.reduceByKey(DeduplicationDate) \
						.map(lambda x: (x[0])+x[1]) 
	repair_balance_order_schemardd = sqlContext.applySchema(repair_balance_vehicle_rdd,dt_repair_order_total_schema)
	repair_balance = sqlContext.parquetFile(HDFS_PATH_DT_REPAIR_ORDER_TOTAL)
	repair_balance.registerTempTable("dt_repair_order_total")
	repair_balance_order_schemardd.insertInto("dt_repair_order_total",overwrite=True)

if __name__ == '__main__':
	getRepairBalanceTable(sc,sqlContext)
