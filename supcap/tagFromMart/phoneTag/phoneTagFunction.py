# -*- coding: utf-8 -*-
import sys
import datetime
from operator import add
         
from mathartsys.supcap.superid.doss_phone_tags import *

#取时间差
def getDifftime(time):
	try:
		today = datetime.datetime.today()
	 	time = datetime.datetime.strptime(time,"%Y-%m-%d %H:%M:%S.0")
		days = (today - time).days
	except Exception, e:
		days = -1
	return days		
#将一条记录多个手机号按照手机号展开
def flatMap(x): 
	return x
#筛选合理的手机号
def getRightPhoneNum(phone):
	# phone_row = ['130','131','132','133','134','135','136','137','138','139','150','151','152','153','155','156','157','158','159','176','177','178','180','181','182','183','184','185','186','187','188','189']
	phone_tag = 1
	if len(phone) == 11:
		if phone[0] == '1':
			phone_tag = 0
		else:
			phone_tag = 1
	elif len(phone) == 12 and phone[0:2] == '01':
		phone_tag = 0
	else:
		phone_tag = 1
	return phone_tag
#将手机号字段合并成list
def getPhoneList(row):
	phone_list = []
	for i in row:
		if getRightPhoneNum(i) == 0 and i not in phone_list:
			if i[0] != '0':
				phone_list.append(i)
			elif i[0] == '0':
				phone_list.append(i[1:])
	return phone_list
#获取手机号下的标签（3个月、6个月、12个月、联系时间距离现在的天数、时间区间）
def getPhoneTags(row):
#(u'2010-12-31 00:00:00.0', u'null')
	tags = [0,0,0,0,0,0,-1,-1]
	day = getDifftime(row[0])
	tags[7] = day
	if day >= 0 and day <= 90:
		tags[0] = 1 
		tags[1] = 1
		tags[2] = 1
		tags[6] = 1
		if row[1] != u'null' and row[1] != u'NULL':
			tags[3] = int(row[1])
			tags[4] = int(row[1])
			tags[5] = int(row[1])
	elif day > 90 and day <= 180:
		tags[1] = 1
		tags[2] = 1
		tags[6] = 2
		if row[1] != u'null' and row[1] != u'NULL':
			tags[4] = int(row[1])
			tags[5] = int(row[1])
	elif day > 180 and day <= 365:
		tags[2] = 1
		tags[6] = 3
		if row[1] != u'null' and row[1] != u'NULL':
			tags[5] = int(row[1])
	elif day > 365:
		tags[0] = 0
		tags[1] = 0
		tags[2] = 0
		tags[6] = 4
	return tuple(tags)
#汇总手机号下的标签
def PhoneTagCount(row1,row2):
	if row1[6] < row2[6]:
		latestDay = (row1[6],row1[7])
	else:
		latestDay = (row2[6],row2[7])
	callCount = (row1[0]+row2[0],row1[1]+row2[1],row1[2]+row2[2],row1[3]+row2[3],row1[4]+row2[4],row1[5]+row2[5])
	return (callCount + latestDay)
#将元组类型转换成字典	
def getPhoneDict(row,rowType):
	tags = {}
	if rowType == 1:
		tags[PHONE_TIMES_INDEALER_LM3] = row[0]
		tags[PHONE_TIMES_INDEALER_LM6] = row[1]
		tags[PHONE_TIMES_INDEALER_LM12] = row[2]
		tags[PHONE_DURATION_INDEALER_LM3] = row[3]
		tags[PHONE_DURATION_INDEALER_LM6] = row[4]
		tags[PHONE_DURATION_INDEALER_LM12] = row[5]
		tags[PHONE_DATE_INDEALER_LATELY] = row[6]
		tags[PHONE_DAYS_INDEALER_LATELY] = row[7]
	elif rowType == 2:
		tags[PHONE_TIMES_CALL_LM3] = row[0]
		tags[PHONE_TIMES_CALL_LM6] = row[1]
		tags[PHONE_TIMES_CALL_LM12] = row[2]
		tags[PHONE_DURATION_CALL_LM3] = row[3]
		tags[PHONE_DURATION_CALL_LM6] = row[4]
		tags[PHONE_DURATION_CALL_LM12] = row[5]
		tags[PHONE_DATE_CALL_LATELY] = row[6]
		tags[PHONE_DAYS_CALL_LATELY] = row[7]
	elif rowType == 3:
		tags[PHONE_TIMES_MARKETING_ACTIVITY] = row
	elif rowType == 4:
		tags[PHONE_TIMES_TEST_DRIVING_LM3] = row[0]
		tags[PHONE_TIMES_TEST_DRIVING_LM6] = row[1] 
		tags[PHONE_TIMES_TEST_DRIVING_LM12] = row[2]
		tags[PHONE_MILES_TEST_DRIVING_LM3] = row[3] 
		tags[PHONE_MILES_TEST_DRIVING_LM6] = row[4]
		tags[PHONE_MILES_TEST_DRIVING_LM12] = row[5]
		tags[PHONE_DATE_TEST_DRIVING_LATELY] = row[6]
		tags[PHONE_DAYS_TEST_DRIVING_LATELY] = row[7]
	return tags

#table join 来店咨询状况+电话咨询状况
def combineDict(dict1,dict2):
	return (dict(dict1,**dict2))




