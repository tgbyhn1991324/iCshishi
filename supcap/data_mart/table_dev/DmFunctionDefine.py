# -*- coding: utf-8 -*-
import sys
import datetime
if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_dataMarketFunctionCollector")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)

from mathartsys.supcap.data_mart.config.table_path import *
from mathartsys.supcap.data_mart.config.table_schema import *
from mathartsys.supcap.data_mart.config.car_mapdefine import *
from mathartsys.supcap.data_mart.table_list.dol_table import *
from mathartsys.supcap.data_mart.table_list.doss_table import *

#根据时间读取文件
'''
功能描述
inputdt:开始读取hdfs的dt
interval:循环读取的天数
根据指定开始读取的时间及天数将读取hdfs上的文件，因为有的数据量巨大，根据dt分区存储
通过此函数参数限定，只读取需要的部分
'''
def getRDDfromTextFilePartitionByDT(sc, filepath,inputdt, interval):
	union_rdd = sc.parallelize([])
	for i in range(0,interval+1):
		dt = datetime.datetime.strptime(inputdt,"%Y%m%d") + datetime.timedelta(i)
		strdt =  datetime.datetime.strftime(dt,"%Y%m%d")
		#dt = datetime.datetime.strptime(inputdt,"%Y%m%d") + datetime.timedelta(i)
		#dt = today - datetime.timedelta(i)
		#strdt =  datetime.datetime.strftime(dt,"%Y%m%d")
		subpath = filepath % (strdt,)
		try:
			cat_tf = sc.textFile(subpath)
			cat_valid_rdd = cat_tf.map(lambda x: x.split("\x01") + [int(strdt),])
			cat_valid_rdd.first()
			union_rdd = union_rdd.union(cat_valid_rdd)
		except Exception, e:
			print subpath, "===does not exists==="
			continue
	return union_rdd

#去重for repair，claim，balance table
'''
输入:(('a',20151101,'2015-11-05 09:59:14:0','2015-11-05 09:59:14:0'),('a',20151101,'2015-11-01 01:30:14:0','2015-11-01 01:30:14:0'),
      ('a',20151102,'2015-11-03 05:30:00:0','2015-11-04 09:59:14:0'),('a',20151102,'2015-11-02 01:40:00:0','2015-11-03 09:59:14:0'))
输出:('a',20151102,'2015-11-02 01:40:00:0','2015-11-03 09:59:14:0')
功能描述:先比较dt字段，取dt大的数据，如果dt相等，再比较生成时间和更新时间，均取最小的。
'''
def DeduplicationDt(a,b):
	chga = list(a)
	if a[-3] > b[-3]:
		return a
	elif a[-3] == b[-3]:
		chga[-2] = min(a[-2],b[-2])
		chga[-1] = min(a[-1],b[-1])
		return tuple(chga)
	else:
		return b

#去重,取更新时间
'''
输入:(('a','2015-11-05 09:59:14:0','2015-11-05 09:59:14:0',-1),('a','2015-11-01 01:30:14:0','2015-11-01 01:30:14:0',-1),
      ('a','2015-11-03 05:30:00:0','2015-11-04 09:59:14:0',-1),('a','2015-11-02 01:40:00:0','2015-11-03 09:59:14:0',-1))
输出:('a','2015-11-01 01:30:14:0','2015-11-05 09:59:14:0',-1)
功能描述:比较同一key值下多条记录的生成时间字段,取最小的生成时间和最大的更新时间
生成时间字段在倒数第三列
'''
def Deduplication(a,b):
	chga = list(a)
	chgb = list(b)
	if a[-3] > b[-3]:
		chga[-3] = chgb[-3]
		return tuple(chga)
	else:
		chgb[-3] = chga[-3]
		return tuple(chgb)

'''
输入:(('a','2015-11-05 09:59:14:0','2015-11-05 09:59:14:0'),('a','2015-11-01 01:30:14:0','2015-11-01 01:30:14:0'),
      ('a','2015-11-03 05:30:00:0','2015-11-04 09:59:14:0'),('a','2015-11-02 01:40:00:0','2015-11-03 09:59:14:0'))
输出:('a','2015-11-01 01:30:14:0','2015-11-05 09:59:14:0')
功能描述:比较同一key值下多条记录的生成时间字段,取最小的生成时间和最大的更新时间
生成时间字段在倒数第二列
'''
def DeduplicationDate(a,b):
	chga = list(a)
	chgb = list(b)
	if a[-2] > b[-2]:
		chga[-2] = chgb[-2]
		return tuple(chga)
	else:
		chgb[-2] = chga[-2]
		return tuple(chgb)

'''
功能描述:
# '''
# def DeduplicationDatastoreDt(a,b):
# 	chga = list(a)
# 	chgb = list(b)
# 	min_date = min(a[-3],b[-3])
# 	if a[-4] > b[-4]:
# 		chga[-3] = min_date
# 		return tuple(chga)
# 	else:
# 		chgb[-3] = min_date
# 		return tuple(chgb)

'''
功能描述:将list中的'null','NULL','Null'替换成None
'''
def clearData(row):
	ret = []
	for i in row:
		if i in ('null','NULL','Null'):
			ret.append(None)
		else:
			ret.append(i)
	return ret

'''
输入:('a', ((1, 2), None))
输出:('a', (1, 2, None, None))
功能描述:将join完的结果数据进行转化
'''
def combineJoin(row,lenRight):
	return (row[0], (row[1][0]  + (row[1][1] or ((None,)*lenRight))))

##购车时间+付款方式 4411:非按揭 4401按揭
'''
输入:(vin号,(时间,'4411'))
输出:(vin号,(时间,'非按揭'))
功能描述:在vin号下打购车时间和付款方式两个标签
'''
def getPaymentValue(row):
	vin = row[0]
	date = row[1][0]
	if row[1][1] == '4411':
		return (vin,(date,u'非按揭'))
	elif row[1][1] == '4401':
		return (vin,(date,u'按揭'))
	else:
		return (vin,(date,None))

'''
输入:(vin号,(时间,'1006101'))
输出:(vin号,(时间,'非按揭'))
功能描述:在vin号下取购车时间和付款方式的值
'''
def getDossPayment(row):
	vin = row[0]
	date = row[1]
	if row[1] == '1006101':
		return (vin,u'非按揭')
	elif row[1] == '1006100':
		return (vin,u'按揭')
	else:
		return (vin,None)

##车型
'''
输入:(vin号,'科鲁兹三厢')
输出:(vin号,('科鲁兹三厢','SEDAN'))
功能描述:在vin号下取车型和类型的值
'''
def getCarTypeValue(row):
	ret = [0,0]
	if row[1]:
		ret[0] = row[1]
		if MAPTYPE.has_key(row[1]):
			ret[1] = MAPTYPE[row[1]]
		else:
			ret[1] = None
	else:
		ret[0] = None
		ret[1] = None
	return (row[0],tuple(ret))

##价格
'''
功能描述:过滤出有效的价格
'''
def filterPrice(x):
	if not x:
		return 1
	if x:
		if x[0:4] in ('null','9999'):
			return 1
	else:
		return 0

##大区
#1：别克 2：凯迪拉克 22：雪佛兰
'''
输入:(vin号,('1','城市','省份'))
输出:(vin号,'销售大区')
功能描述:在vin号下取销售大区的值
'''
def getAreaValue(row):
	ret = 0
	brand_id = row[1][0]
	if row[1][2]:
		province = row[1][2]
		if brand_id == '1':
			if MAPAREA_B.has_key(province):
				ret = MAPAREA_B[province]
			else:
				ret = None
		elif brand_id == '2':
			if MAPAREA_C.has_key(province):
				ret = MAPAREA_C[province]
			else:
				ret = None
		elif brand_id == '22':
			if MAPAREA_X.has_key(province):
				ret = MAPAREA_X[province]
			else:
				ret = None
		else:
			ret = None
	else:
		ret = None
	return (row[0],ret)

#4301个人客户 4311机构客户
'''
功能描述:在vin号下取客户类型的值
'''
def getCustomerTypeValue(row):
	if row[1] == '4301':
		return (row[0],u'个人客户')
	elif row[1] == '4311':
		return (row[0],u'机构客户')
	else:
		return (row[0],None)

'''
功能描述:将dol的品牌转换成与doss一致
'''
def chgBrandCode(row):
	brand_id = row[1][0]
	brand_chg = 0
	if brand_id == '22':
		brand_chg = '2'
	elif brand_id == '2':
		brand_chg = '3'
	elif brand_id == '1':
		brand_chg = '1'
	else:
		brand_chg = None
	return (row[0],brand_chg)

'''
功能描述:获取有效的购车日期
'''
def getRightDate(date1,date2):
	if date1 != u'null':
		return date1
	elif date2 != u'null':
		return date2
	else:
		return None

#合并字典
'''
功能描述:将两个字典合并成一个字典
'''
def combineDict(dict1,dict2):
	ret = {}
	ret = dict(dict1,**dict2)
	return ret

#合并数据
'''
功能描述:比较生成日期，取最小的生成日期和最大的更新日期
'''
def DeduplicationDict(a,b):
	if a[0]['create_date'] > b[0]['create_date']:
		a[0]['create_date'] = b[0]['create_date']
		return a
	else:
		b[0]['create_date'] = a[0]['create_date']
		return b

#合并join之后的字典
'''
功能描述:合并join之后的字典
'''
def joinDict(row):
	if row[1]:
		return (dict(row[0],**row[1]))
	else:
		return (row[0])

#是否二手车
'''
功能描述:取包含'UCDMS'的记录
'''
def getUsedCar(row1,row2):
	UsedCar = 0
	if row1 == u'UCDMS':
		UsedCar = row1
	if row2 == u'UCDMS':
		UsedCar = row2
	return  UsedCar

'''
功能描述:取是否是二手车的值
'''
def getUsedCarTag(row):
	if row[1] == u'UCDMS':
		return (row[0],u"是")
	else:
		return (row[0],u"否")

#是否是会员 VIN_IF_MEMBERSHIP
'''
功能描述:取是否是会员的值
'''
#列转行
def listChgRow(row1, row2):
	ret = {}
	for key1 in row1.keys():
		ret[key1] = row1[key1]
	for key2 in row2.keys():
		ret[key2] = row2[key2]
	return ret
#u'5934170'雪佛兰会员，u'5934168'别克会员，u'5934169'凯迪拉克会员
def getMembershipTag(row):
	ret = [0,0]
	ret[0] = row.get(u'1395304',0)
	ret[1] = u'是' if (row.get(u'5934170') or row.get(u'5934168') or row.get(u'5934169')) else u'否'
	return ret  

#是否带Onstar标签
'''
功能描述:取是否带Onstar的值
'''
def getOnstarTag(row):
	onstarTag = None
	if row[1] == u"Y" or row[1] == u"YES":
		onstarTag = u'是'
	elif row[1] == u"N" or row[1] == u"NO":
		onstarTag = u'否'
	else:
		if row[1] not in ('null','NULL','Null',None): 
			onstarTag = u'其他'
		else:
			onstarTag = None
	return (row[0],onstarTag)

#取最近里程数及最近进站日期
'''
功能描述:取最近里程数及最近进站日期
'''
def getLatestMileage(row1,row2):
	try:
		a = float(row1[0])
	except:
		a = 200001
	if row1[1] > row2[1] and a <= 200000:
		return row1
	else:
		return row2

'''
功能描述:将idm库下的josn存储格式的文件展开来存储
'''
def getIdmTags(row):
	import json
	ret = [None]*56
	member_info = {}
	auto_vh_info = {}
	card_info = {}
	contact_info = {}
	ret[0] = row[0]
	ret[1] = row[1].get(u'5934148') #RID
	ret[2] = row[1].get(u'1395304') #VIN1
	ret[3] = row[1].get(u'1660291') #QQ
	ret[4] = row[1].get(u'5934146') #车主类别 ACCOUNT_CLASS 
	ret[5] = row[1].get(u'5934175') #车主类型 ACCOUNT_TYPE
	ret[6] = row[1].get(u'5934149') #sex
	ret[7] = row[1].get(u'5934171') #name
	ret[8] = row[1].get(u'5934177') #birthday
	ret[9] = row[1].get(u'14')      #city
	ret[10] = row[1].get(u'5934170') or row[1].get(u'5934168') or row[1].get(u'5934169') #是否会员 IF_MEMBERSHIP
	member_info_temp = row[1].get(u'5934153',0)
	if member_info_temp != 0:
		try:
			member_info = json.loads(member_info_temp)[0]
		except:
			member_info = {}
		auto_vh_info = member_info.get(u'AUTO_VH_INFO',0)
		if auto_vh_info != 0:
			auto_vh_info = member_info.get(u'AUTO_VH_INFO')[0]
			ret[11] = auto_vh_info.get("AUTO_ID")
			ret[12] = auto_vh_info.get("AUTO_NUM")
			ret[13] = auto_vh_info.get("AUTO_STATUS")
			ret[14] = auto_vh_info.get("CAR_LINSENCE")
			ret[15] = auto_vh_info.get("CONTACT_BK_PHONE")
			ret[16] = auto_vh_info.get("CONTACT_NAME")
			ret[17] = auto_vh_info.get("CONTACT_PHONE")
			ret[18] = auto_vh_info.get("DEALER")
			ret[19] = auto_vh_info.get("OPERATOR_NAME")
			ret[20] = auto_vh_info.get("ORG_FLG")
			ret[21] = auto_vh_info.get("SALE_DT")
			ret[22] = auto_vh_info.get("VIN")
			ret[23] = auto_vh_info.get("MODEL_NAME")
			ret[24] = auto_vh_info.get("COLOR")
			ret[25] = auto_vh_info.get("SERIES_NAME")
			ret[26] = auto_vh_info.get("PACKAGE")
		card_info = member_info.get(u'CARD_INFO',0)
		if card_info != 0:
			card_info = member_info.get(u'CARD_INFO',0)[0]
			ret[27] = card_info.get("CANCEL_DT")
			ret[28] = card_info.get("CANCEL_REASON")
			ret[29] = card_info.get("CARD_ID")
			ret[30] = card_info.get("CARD_NUM")
			ret[31] = card_info.get("CARD_STATUS")
			ret[32] = card_info.get("CARD_TYPE")
			ret[33] = card_info.get("ENTITY_FLG")
			ret[34] = card_info.get("PUBLISH_DT")
			ret[35] = card_info.get("REG_DT")
		contact_info = member_info.get(u'CONTACT_INFO',0)
		if contact_info != 0:
			contact_info = member_info.get(u'CONTACT_INFO',0)[0]
			ret[36] = contact_info.get("ACCOUNT_REL_TYPE")
			ret[37] = contact_info.get("ADDR")
			ret[38] = contact_info.get("CELL_PH_NUM")
			ret[39] = contact_info.get("CONTACT_ID")
			ret[40] = contact_info.get("EMAIL_ADDR")
			ret[41] = contact_info.get("EMAIL_FLG")
			ret[42] = contact_info.get("NAME")
			ret[43] = contact_info.get("SEX_MF")
			ret[44] = contact_info.get("SMS_FLG")
			ret[45] = contact_info.get("TYPE")
			ret[46] = contact_info.get("WEIXIN_FLG")
			ret[47] = contact_info.get("WEIXIN_NUM")
			ret[48] = contact_info.get("UPD_DT")
		ret[49] = member_info.get(u'BRAND')
		ret[50] = member_info.get(u'MEMBER_ID')
		ret[51] = member_info.get(u'OPEN_ID')
		ret[52] = member_info.get(u'STATUS_CD')
		ret[53] = member_info.get(u'updateflag')
		ret[54] = member_info.get(u'ACTIVED_DATE')
		ret[55] = member_info.get(u'ACTIVED_FROM')
	return tuple(ret)

'''
功能描述:筛选合理的手机号码,11位以1开头，12位以01开头的号码为暂定的合理手机号码
'''
def getRightPhoneNum(phone):
	# phone_row = ['130','131','132','133','134','135','136','137','138','139','150','151','152','153','155','156','157','158','159','176','177','178','180','181','182','183','184','185','186','187','188','189']
	phone_tag = 1
	if phone:
		for i in phone:
			if len(i) == 11:
				if i[0] == '1':
					phone_tag = 0
					break
				else:
					phone_tag = 1
			elif len(i) == 12:
				if i[0:2] == '01':
					phone_tag = 0
					break
				else:
					phone_tag = 1
			else:
				phone_tag = 1
	return phone_tag



