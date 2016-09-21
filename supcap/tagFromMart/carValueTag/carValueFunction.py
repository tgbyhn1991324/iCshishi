# -*- coding: utf-8 -*-
import sys
import datetime

from mathartsys.supcap.tagFromMart.tag_name.vin_tags import *
from mathartsys.supcap.tagFromMart.config.car_mapdefine import * 
#获得标签其他值，用-1表示
def getOtherValue(cartag):
	if cartag not in ('null','NULL','Null',None):
		return -1
	else:
		return None
#获得车辆属性表
def getCarTagCollector(row,series_map):
	tags = {}
	tagsTime = {}
	tags["tag_id"] = row.vin
	tags[VIN_BRAND] = int(row.brand_name) if row.brand_name in ('1','2','3') else getOtherValue(row.brand_name)
	tags[VIN_SALES_AREA] = MAPAREA.get(row.sales_area_name,getOtherValue(row.sales_area_name))
	tags[VIN_SALES_PROVINCE] = MAPPROVINCE.get(row.sales_province_name,getOtherValue(row.sales_province_name))
	tags[VIN_SALES_CITY] = row.sales_city_name
	tags[VIN_SALES_TYPE] = MAPSALESTYPE.get(row.customer_type,getOtherValue(row.customer_type))
	tags[VIN_PAY_METHOD] = MAPPAYMETHORD.get(row.pay_method,getOtherValue(row.pay_method))
	tags[VIN_CAR_MODEL] =  series_map.get(row.car_model_name,getOtherValue(row.car_model_name))	
	tags[VIN_CAR_TYPE] = MAPTYPE.get(row.car_type_name,getOtherValue(row.car_type_name))
	tags[VIN_CAR_COLOR] = MAPCOLOR.get(row.car_color_name,getOtherValue(row.car_color_name))
	tags[VIN_IF_ONSTAR] = MAPYESNO.get(row.if_onstar,getOtherValue(row.if_onstar))
	tags[VIN_IF_MEMBERSHIP] = MAPYESNO.get(row.if_membership,getOtherValue(row.if_membership))
	tags[VIN_IF_USED_CAR] = MAPYESNO.get(row.if_used_car,getOtherValue(row.if_used_car))
	tags[VIN_RETAIL_PRICE] = getPriceTag(row.retail_price)
#购车年龄段、购车年份、购车时间、当前里程、是否出保、即将出保时间、月均里程、年里程数值标签
	valuelist = [row.rid,row.brand_name,row.buy_date,row.mileage_latest,row.start_date_latest] 
	tagsTime = getVinTagFromDatastore(valuelist)
	return dict(tags,**tagsTime)
#购车年龄段、购车年份、购车时间、当前里程、是否出保、即将出保时间、月均里程、年里程数值标签
#[u'342423199103058393',u'2',u'2013-01-17 00:00:00.0', u'7000', u'2013-11-03 00:00:00.0']
def getVinTagFromDatastore(valuelist):
	tags = {}
	day1 = 0
	day2 = 0
	try:
		id_year = int(valuelist[0][6:10])
	except Exception, e:
		age = -1
		id_year = -1
	try:
		buy_time_year = int(valuelist[2][0:4])
		tags[VIN_BUY_YEAR] = buy_time_year 
		if id_year != -1:
			age = buy_time_year - id_year
		else:
			age = -1
	except Exception, e:
		age = -1
		tags[VIN_BUY_YEAR] = -1
	# tags[VIN_BUY_AGE] 购车年龄段
	if age >= 18 and age <= 25:
		tags[VIN_BUY_AGE] = 1
	elif age > 25 and age <= 30:
		tags[VIN_BUY_AGE] = 2
	elif age > 30 and age <= 35:
		tags[VIN_BUY_AGE] = 3
	elif age > 35 and age <= 40:
		tags[VIN_BUY_AGE] = 4
	elif age > 40 and age <= 45:
		tags[VIN_BUY_AGE] = 5
	elif age > 45 and age <= 50:
		tags[VIN_BUY_AGE] = 6
	elif age > 50 and age <= 60:
		tags[VIN_BUY_AGE] = 7
	elif age > 60:
		tags[VIN_BUY_AGE] = 8
	else:
		if valuelist[2] not in ('null','NULL','Null',None) or valuelist[0] not in ('null','NULL','Null',None):
			tags[VIN_BUY_AGE] = -1
		else:
			tags[VIN_BUY_AGE] = None
	try:
		today = datetime.datetime.today()
	 	time = datetime.datetime.strptime(valuelist[2],"%Y-%m-%d %H:%M:%S.0")
		day1 = (today - time).days  #购买时间距离现在的天数
		m1 = day1/30
	except Exception, e:
		m1 = -1	
	try:
		time = datetime.datetime.strptime(valuelist[2],"%Y-%m-%d %H:%M:%S.0")
	 	start_time = datetime.datetime.strptime(valuelist[4],"%Y-%m-%d %H:%M:%S.0")
		day2 = (start_time - time).days #购买时间距离进站维修的天数
	except Exception, e:
		day2 = -1
	#tags[VIN_BUY_TIME]购车时间
	if m1 >= 0 and m1 <= 1 :
		tags[VIN_BUY_TIME] = 1
	elif m1 > 1 and m1 <= 3 :
		tags[VIN_BUY_TIME] = 2
	elif m1 > 3  and m1 <= 6:
		tags[VIN_BUY_TIME] = 3
	elif m1 > 6 and m1 <= 12:
		tags[VIN_BUY_TIME] = 4
	else:
		if valuelist[2] not in ('null','NULL','Null',None):
			tags[VIN_BUY_TIME] = -1
		else:
			tags[VIN_BUY_TIME] = None
	#tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] 当前里程
	if day2 > 0:
		try:
			mileage = (float(valuelist[3])*day1)/day2
			month_avg_mileage = (float(valuelist[3])*30)/day2
			tags[VIN_YEAR_AVG_MILEAGE] = int(month_avg_mileage*12)
		except Exception, e:
			mileage = 0 
			month_avg_mileage = 0
	else:
		mileage = 0
		month_avg_mileage = 0
	if  0 < mileage <= 10000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 1
	elif mileage > 10000 and mileage <= 20000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 2
	elif mileage > 20000 and mileage <= 30000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 3
	elif mileage > 30000 and mileage <= 50000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 4
	elif mileage > 50000 and mileage <= 70000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 5
	elif mileage > 70000 and mileage <= 100000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 6
	elif mileage > 100000 and mileage <= 150000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 7
	elif mileage > 150000 and mileage <= 250000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 8
	elif mileage > 250000:
		tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = 9
	else:
		if valuelist[3] not in ('null','NULL','Null',None):
			tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = -1
			tags[VIN_YEAR_AVG_MILEAGE] = -1
		else:
			tags[VIN_CURRENT_YEAR_MILEAGE_RANGE] = None
			tags[VIN_YEAR_AVG_MILEAGE] = None
	#tags[VIN_IF_CAR_INSURANCE]是否出保 tags[VIN_INSURANCE_DATE] 即将出保时间
	#BRAND_ID="1"----Buick；BRAND_ID="2"----Chevrolet;BRAND_ID="3"--Cadillca；
	brand = valuelist[1]
	if m1 == -1:
		tags[VIN_IF_CAR_INSURANCE] = -1
		tags[VIN_INSURANCE_DATE] = -1
	else:
		if brand == u'3':
			if m1 > 36 or (datetime.datetime.strptime(valuelist[2],"%Y-%m-%d %H:%M:%S.0") < datetime.datetime(2013,9,1) and mileage > 100000):
				tags[VIN_IF_CAR_INSURANCE] = 1
				tags[VIN_INSURANCE_DATE] = None
			elif m1 >= 33 and m1 <= 36:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = 1
			elif m1 > 30 and m1 <= 33:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = 2
			elif m1 > 24 and m1 <= 30:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = 3		
			elif m1 >= 0 and m1 <= 24:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = -1
			else:			
				if valuelist[3] not in ('null','NULL','Null',None):
					tags[VIN_IF_CAR_INSURANCE] = -1
					tags[VIN_INSURANCE_DATE] = -1
				else:
					tags[VIN_IF_CAR_INSURANCE] = None
					tags[VIN_INSURANCE_DATE] = None
		elif brand in ('1','2'):
			if m1 > 36 or mileage > 100000 or datetime.datetime.strptime(valuelist[2],"%Y-%m-%d %H:%M:%S.0") < datetime.datetime(2013,9,1):
				tags[VIN_IF_CAR_INSURANCE] = 1
				tags[VIN_INSURANCE_DATE] = None
			elif 33 <= m1 <= 36 or 90000 <= mileage <=100000:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = 1
			elif 30 <= m1 < 33 or 80000 <= mileage < 90000:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = 2
			elif 0 <= m1 <= 30 and 0 < mileage <= 100000:
				tags[VIN_IF_CAR_INSURANCE] = 2
				tags[VIN_INSURANCE_DATE] = -1		
			else:
				if valuelist[3] not in ('null','NULL','Null',None):
					tags[VIN_IF_CAR_INSURANCE] = -1
					tags[VIN_INSURANCE_DATE] = -1
				else:
					tags[VIN_IF_CAR_INSURANCE] = None
					tags[VIN_INSURANCE_DATE] = None
	#tags[VIN_MONTH_AVG_MILEAGE_RANGE]月均里程
	if month_avg_mileage > 0 and month_avg_mileage <= 500:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 1
	elif month_avg_mileage > 500 and month_avg_mileage <= 1000:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 2
	elif month_avg_mileage > 1000 and month_avg_mileage <= 1500:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 3
	elif month_avg_mileage > 1500 and month_avg_mileage <= 2000:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 4
	elif month_avg_mileage > 2000 and month_avg_mileage <= 2500:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 5
	elif month_avg_mileage > 2500 and month_avg_mileage <= 3000:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 6
	elif month_avg_mileage > 3000:
		tags[VIN_MONTH_AVG_MILEAGE_RANGE] = 7
	else:
		if mileage not in ('null','NULL','Null',None):
			tags[VIN_MONTH_AVG_MILEAGE_RANGE] = -1
		else:
			tags[VIN_MONTH_AVG_MILEAGE_RANGE] = None
	return tags

#零售价标签
def getPriceTag(retail_price):
	#price_tag 零售价
	if retail_price not in ('null','NULL','Null',None):
		try:
			price = float(retail_price)
		except Exception, e:
			price = 0
		if price >= 50000 and price < 80000:
			price_tag = 1
		elif price >= 80000 and price < 100000:
			price_tag = 2
		elif price >= 100000 and price < 150000:
			price_tag = 3
		elif price >= 150000 and price < 200000:
			price_tag = 4
		elif price >= 200000 and price < 250000:
			price_tag = 5
		elif price >= 250000 and price < 350000:
			price_tag = 6
		elif price >= 350000 and price < 500000:
			price_tag = 7
		elif price >= 500000 and price < 700000:
			price_tag = 8
		elif price >= 700000 and price < 1000000:
			price_tag = 9
		elif price >= 1000000:
			price_tag = 10
		else:
			price_tag = -1
	else:
		price_tag = None
	return price_tag