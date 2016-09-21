# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_vehicleTableACollector")
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

def dolSubDossTableGet(sc,sqlContext):
	#####dol有doss没有的VIN号属性取值
	tdv_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_VEHICLE)
	tdv_valid_rdd = tdv_tf.map(lambda x: x.split("\x01"))
	doss_tdv_rdd = tdv_valid_rdd.filter(lambda x: len(x[TDV_VIN]) == 17).map(lambda x: (x[TDV_VIN],[])) 
	tdvs_tf = sc.textFile(HDFS_PATH_DOL_TT_DOL_VEHICLE_SALE) 
	tdvs_valid_rdd = tdvs_tf.map(lambda x: x.split("\x01"))
	dol_tdvs_rdd = tdvs_valid_rdd.filter(lambda x: len(x[2]) == 17 and x[2][0] != '-').map(lambda x: (x[2],x))
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	try:
		dolSubDossVin_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_SUB_DOSS_VIN).map(lambda x: (x[0],(x[1:] +(-1,))))
	except:
		dolSubDossVin_temp_rdd = sc.parallelize([])
	dolSubDossVin_rdd = dol_tdvs_rdd.subtractByKey(doss_tdv_rdd).map(lambda x: x[1]) \
							.map(lambda x: (x[TDVS2_VEHICLE_ID],x[TDVS2_VN],x[TDVS2_VIN],x[TDVS2_PVI],x[TDVS2_ENGINE_NUMBER],x[TDVS2_TRIM_COLOR],x[TDVS2_TRIM_LEVEL],x[TDVS2_EXTERNAL_COLOR],x[TDVS2_DELIVERY_DATE],x[TDVS2_MILEAGE],x[TDVS2_LICENSE_NUMBER],x[TDVS2_NODE_DATE],x[TDVS2_VHCL_NODE],x[TDVS2_LOCATION],x[TDVS2_SCHEDULE_DATE],x[TDVS2_MODEL_YEAR],x[TDVS2_LIFE_CYCLE],x[TDVS2_ATTACHMENT],x[TDVS2_NODE_STATUS],x[TDVS2_HOLD_STATUS],x[TDVS2_INIT_VDC],x[TDVS2_IS_VIP],x[TDVS2_VIP_COMMENT],x[TDVS2_VIP_DATE],x[TDVS2_VN_CREATE],x[TDVS2_VHCL_TYPE],x[TDVS2_CUSTOMER_ID],x[TDVS2_ORG_ID],x[TDVS2_BRAND_ID],x[TDVS2_CORP_ID],x[TDVS2_EFFECTIVE_DATE],x[TDVS2_END_DATE],x[TDVS2_VHCL_NODE_DESC],x[TDVS2_HOLD_STATUS_DESC],x[TDVS2_VHCL_TYPE_DESC],x[TDVS2_LIFE_CYCLE_DESC],x[TDVS2_PRODUCT_ID])) \
							.map(lambda x: (x[0],(x[1:] +(today_str,today_str,1)))) \
							.union(dolSubDossVin_temp_rdd).reduceByKey(lambda x,y:Deduplication(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: ((x[0],)+x[1][0:-1])).coalesce(20)
	dolSubDossVin_rows_schemardd = sqlContext.applySchema(dolSubDossVin_rdd, dolSubDossVin_schema)
	dolSubDossVin_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_SUB_DOSS_VIN)
	dolSubDossVin_schemardd.registerTempTable("dol_sub_doss_vin")
	dolSubDossVin_rows_schemardd.insertInto("dol_sub_doss_vin",overwrite=True)

def vehicleTableAGet(sc,sqlContext):
	##品牌标签
	dolSubDoss_rdd = sqlContext.parquetFile(HDFS_PATH_DM_DOL_SUB_DOSS_VIN)
	dol_brand_rdd = dolSubDoss_rdd.map(lambda x: (x.vin,(x.brand_id,)))
	##购车时间+付款方式 4411:非按揭 4401按揭
	dolSubDoss_saleTime_rdd = dolSubDoss_rdd.map(lambda x: (x.vehicle_id,x.vin)) 
	ddso_pf = sqlContext.parquetFile(HDFS_PATH_DM_DOL_DEALER_SALES_ORDER)
	ddso_rdd = ddso_pf.map(lambda x: (x.vehicle_id,(x.invoice_date,x.payment_type)))
	dol_saleTime_rows = dolSubDoss_saleTime_rdd.join(ddso_rdd).map(lambda x: getPaymentValue(x[1])) \
							.map(lambda x: (x[0],{'buy_date':x[1][0],'pay_method':x[1][1]}))
	##颜色标签
	dolSubDoss_product_rdd = dolSubDoss_rdd.map(lambda x: (x.product_id,x.vin)) 
	dpvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_VALUE_DTL)
	dpvd_rdd = dpvd_pf.map(lambda x: (x.product_id,x.color_id))
	dcvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COLOR_VALUE_DTL)
	dcvd_rdd = dcvd_pf.map(lambda x: (x.color_id,x.chinese_name))
	dol_color_rows = dolSubDoss_product_rdd.join(dpvd_rdd).map(lambda x: (x[1][1],x[1][0])) \
							.join(dcvd_rdd).map(lambda x: (x[1][0],{'car_color_name':x[1][1]}))
	##车型标签
	dprvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_VALUE_DTL)
	dprvd_rdd = dpvd_pf.map(lambda x: (x.product_id,x.package_id))
	dpavd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PACKAGE_VALUE_DTL)
	dpavd_rdd = dpavd_pf.map(lambda x: (x.package_id,x.model_id))
	dmvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_MODEL_VALUE_DTL)
	dmvd_rdd = dmvd_pf.map(lambda x: (x.model_id,x.series_id))
	dsvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_SERIES_VALUE_DTL)
	dsvd_rdd = dsvd_pf.map(lambda x: (x.series_id,x.chinese_name))
	dol_series_rows = dolSubDoss_product_rdd.join(dprvd_rdd).map(lambda x: (x[1][1],x[1][0])) \
								.join(dpavd_rdd).map(lambda x: (x[1][1],x[1][0])) \
								.join(dmvd_rdd).map(lambda x: (x[1][1],x[1][0])) \
								.join(dsvd_rdd).map(lambda x: x[1]).map(lambda x: getCarTypeValue(x)) \
								.map(lambda x: (x[0],{'car_model_name':x[1][0],'car_type_name':x[1][1]})) #(u'1G6A95SX5E0112823', (u'ATS', u'SEDAN'))
	##价格标签
	dppvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PRODUCT_PRICE_VALUE_DTL)
	# dppvd_rdd = dppvd_pf.filter(lambda x: x.price_type == '2').filter(lambda x: filterPrice(x.face_out_date)==1).map(lambda x: (x.product_id,x.price))
	dppvd_rdd = dppvd_pf.map(lambda x: (x.product_id,x.price,x.price_type,x.face_out_date)).filter(lambda x: x[2] == '2' and filterPrice(x[3]) == 1) \
						.map(lambda x: x[0:2])
	dol_price_rows = dolSubDoss_product_rdd.join(dppvd_rdd).map(lambda x: (x[1][0],{'retail_price':x[1][1]}))
	##销售省份和城市标签
	dolSubDoss_veh_rdd = dolSubDoss_rdd.map(lambda x: (x.vehicle_id,x.vin))
	ddso_rdd = ddso_pf.map(lambda x: (x.vehicle_id,x.org_id))
	ddovd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_DPT_ORG_VALUE_DTL)
	ddovd_rdd = ddovd_pf.map(lambda x: (x.org_id,x.company_id))
	dprvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_COMPANY_VALUE_DTL)
	dprvd_rdd = dprvd_pf.map(lambda x: (x.company_id,x.city_id))
	dcvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CITY_VALUE_DTL)
	dcvd_rdd = dcvd_pf.map(lambda x: (x.city_id,(x.city_name,x.province_id)))
	dpvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_PROVINCE_VALUE_DTL)
	dpvd_rdd = dpvd_pf.map(lambda x: (x.province_id,x.province_name))
	dol_city_rows = dolSubDoss_veh_rdd.join(ddso_rdd).map(lambda x: (x[1][1],x[1][0])) \
									.join(ddovd_rdd).map(lambda x: (x[1][1],x[1][0])) \
									.join(dprvd_rdd).map(lambda x: (x[1][1],x[1][0])) \
									.join(dcvd_rdd).map(lambda x: (x[1][1][1],(x[1][1][0],x[1][0]))) \
									.leftOuterJoin(dpvd_rdd).map(lambda x:(x[1][0][1],(x[1][0][0],x[1][1])))
	dol_city_dict =dol_city_rows.map(lambda x:(x[0],{'sales_city_name':x[1][0],'sales_province_name':x[1][1]}))
	##大区标签
	#1：别克 2：凯迪拉克 22：雪佛兰
	dol_area_rows = dol_brand_rdd.join(dol_city_rows).map(lambda x: combineJoin(x,2)) \
									.map(lambda x: getAreaValue(x)).map(lambda x: (x[0],{'sales_area_name':x[1]}))
	#4301个人客户 4311机构客户
	#客户类型标签
	dolSubDoss_customer_rdd = dolSubDoss_rdd.map(lambda x: (x.customer_id,x.vin))
	dcvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOL_CUSTOMER_VALUE_DTL).map(lambda x: (x.customer_id,x.customer_type))
	dol_customer_rows = dolSubDoss_customer_rdd.join(dcvd_pf).map(lambda x: getCustomerTypeValue(x[1])) \
									.map(lambda x: (x[0],{'customer_type':x[1]}))
	##dolSubDossVin车辆属性信息
	#vin,品牌、车型、类型、价格、颜色、销售大区、城市、省份、购车时间、付款方式、客户类型
	dol_car_rows = dol_brand_rdd.map(lambda x: chgBrandCode(x)).map(lambda x: (x[0],{'brand_name':x[1]})) \
								.union(dol_series_rows).union(dol_price_rows).union(dol_color_rows) \
								.union(dol_area_rows).union(dol_city_dict).union(dol_saleTime_rows) \
								.union(dol_customer_rows).coalesce(300)
	#DM_VEHICLE_VALUE_DTL基础表
	dvvd_pf = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_VALUE_DTL) #10316207
	#零售价标签
	lprvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_PRODUCT_VALUE_DTL)
	lprvd_temp_rows = lprvd_pf.map(lambda x: (x.src_product_id,x.msrp)) 
	dvvd_price_rdd = dvvd_pf.map(lambda x: (x.product_id,x.vin)) 
	vin_price_rows = dvvd_price_rdd.join(lprvd_temp_rows).map(lambda x: (x[1][0],{'retail_price':x[1][1]}))
	#品牌标签 
	vin_brand_rows = dvvd_pf.map(lambda x: (x.vin,{'brand_name':x.brand_id}))
	#销售大区、销售城市、销售省份标签
	ldvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_DEALER_VALUE_DTL)
	ldvd_temp_rows = ldvd_pf.map(lambda x: (x.src_dealer_id,(x.city_id,x.partner_region)))
	dvvd_saleArea_rows = dvvd_pf.map(lambda x: (x.dealer_id,(x.vin,x.brand_id)))
	lcvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_CITY_VALUE_DTL)
	lcvd_vin_rows = lcvd_pf.map(lambda x: (x.src_id,(x.city_name_cn,x.province_name_cn))).distinct()
	vin_AreaCity_rows = dvvd_saleArea_rows.leftOuterJoin(ldvd_temp_rows).map(lambda x: combineJoin(x,2)) \
										.map(lambda x: (x[1][2],(x[1][0],x[1][1],x[1][3]))) \
										.leftOuterJoin(lcvd_vin_rows).map(lambda x: combineJoin(x,2)) \
										.map(lambda x: (x[1][0],{'sales_area_name':x[1][2],'sales_city_name':x[1][3],'sales_province_name':x[1][4]}))
	#付款方式标签
	tdro_tf = sc.textFile(HDFS_PATH_DOSS_TT_DOSS_RETAIL_ORDER)
	tdro_valid_rdd = tdro_tf.map(lambda x: x.split("\x01"))
	tdro_tags_rows = tdro_valid_rdd.filter(lambda x:x[TDRO_VEHICLE_ID] != u'null' and x[TDRO_IS_EFF] == '1' and x[TDRO_STATUS] == '6').map(lambda x: (x[TDRO_VEHICLE_ID],x[TDRO_PAYMENT_METHOD])).distinct()
	dvvd_payMethod_rows = dvvd_pf.map(lambda x: (x.vehicle_id,x.vin))
	vin_payMethod_rows = dvvd_payMethod_rows.join(tdro_tags_rows).map(lambda x: getDossPayment(x[1])).map(lambda x: (x[0],{'pay_method':x[1]})) 
	#车型、类型标签
	lsvd_pf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_SERIES_VALUE_DTL)
	lsvd_temp_rows = lsvd_pf.map(lambda x: ((x.src_series_id,x.brand_id),x.series_name)) #((u'2', u'3'), u'XTS') #53
	dvvd_carType_rows = dvvd_pf.map(lambda x: ((x.series_id,x.brand_id),x.vin))
	vin_carType_rows = dvvd_carType_rows.join(lsvd_temp_rows).map(lambda x: getCarTypeValue(x[1])).map(lambda x: (x[0],{'car_model_name':x[1][0],'car_type_name':x[1][1]}))
	#车身颜色标签
	lcvd_tf = sqlContext.parquetFile(HDFS_PATH_LKUP_DOSS_COLOR_VALUE_DTL)
	lcvd_temp_rows = lcvd_tf.map(lambda x: (x.src_color_id,x.color_name)) #218
	dvvd_carColor_rows = dvvd_pf.map(lambda x: (x.color_id,x.vin))
	vin_color_rows = dvvd_carColor_rows.join(lcvd_temp_rows).map(lambda x: (x[1][0],{'car_color_name':x[1][1]})) #(u'146', (u'LSGPB54U5BS129723', u'\u949b\u91d1\u7070'))
	#购车时间
	#(u'LSGWG52Z14S238808', (u'2004-05-05 00:00:00.0', u'1'))
	dvvd_time_rows = dvvd_pf.map(lambda x: (x.vin,getRightDate(x.invoice_date,x.retail_date))).map(lambda x: (x[0],{'buy_date':x[1]})) 
	#客户类型
	cat_tf = sc.textFile(HDFS_PATH_CEM_CX_ASSET_TT)
	cat_valid_rdd = cat_tf.map(lambda x: x.split("\x01"))
	cat_customer_rows = cat_valid_rdd.filter(lambda x: len(x[CAT_VIN]) == 17).map(lambda x: (x[CAT_VIN],x[CAT_ACCOUNT_TYPE])).distinct().map(lambda x: (x[0],{'customer_type':x[1]})) 
	#vin,品牌、车型、类型、价格、颜色、销售大区、城市、省份、购车时间、付款方式、客户类型
	doss_car_rows = vin_brand_rows.union(vin_carType_rows).union(vin_price_rows).union(vin_color_rows) \
								.union(vin_AreaCity_rows).union(dvvd_time_rows).union(vin_payMethod_rows) \
								.union(cat_customer_rows).coalesce(500)
	#vehicle_info_a doss系统属性
	try:
		car_temp_rdd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_A).map(lambda x: (x.vin,(x.asDict(),-1)))
	except:
		car_temp_rdd = sc.parallelize([])
	today_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S:0')
	car_rows = doss_car_rows.union(dol_car_rows).reduceByKey(combineDict) \
							.map(lambda x: (x[0],dict({'vin':x[0]},**x[1]))) \
							.map(lambda x: (x[0],(dict(x[1],**{'create_date':today_str,'update_date':today_str}),1))) \
							.union(car_temp_rdd).reduceByKey(lambda x,y:DeduplicationDict(x,y)).filter(lambda x: x[1][-1] == 1) \
							.map(lambda x: x[1][0]).filter(lambda x: x.get('brand_name') != None) \
							.map(lambda x: formatRowFromSchema(x, dm_vehicle_info_a_schema)).coalesce(100)
	car_rows_schemardd = sqlContext.applySchema(car_rows, dm_vehicle_info_a_schema)
	car_schemardd = sqlContext.parquetFile(HDFS_PATH_DM_VEHICLE_INFO_A)
	car_schemardd.registerTempTable("dm_vehicle_info_a")
	car_rows_schemardd.insertInto("dm_vehicle_info_a",overwrite=True)

if __name__ == '__main__':
	dolSubDossTableGet(sc,sqlContext)
	vehicleTableAGet(sc,sqlContext)
