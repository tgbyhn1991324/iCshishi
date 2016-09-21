# -*- coding: utf-8 -*-
import sys
import time 
import datetime
if __name__ == '__main__':
	sys.path.insert(0, "../../../")

from mathartsys.supcap.config.mapdefine_person import *
from mathartsys.supcap.config.person_config import *
from mathartsys.supcap.superid.rid_tags import *

def getRIDTags(row):
	"""
	input:
		row: is a specific arraylist values of the table(cx_asset_tt) row
	output:
		tuple(id, map): id is the unique id for person, map is the tags(key: value) for the id
	"""
	ret = ()
	tem_tag = {}
	#姓名标签
	tem_tag[RID_NAME] = row.customer_name
	#性别标签
	try:
		if (int(row.rid[-2:-1])) % 2 == 1:
			tem_tag[RID_SEX] = 1
		elif (int(row.rid[-2:-1])) % 2 == 0:
			tem_tag[RID_SEX] = 2
	except Exception, e:
		tem_tag[RID_SEX] = -1
	#年龄标签
	try:
		age_in_ID = int(time.strftime("%Y",time.localtime())) - int(row.rid[6:10])
	except Exception, e:
		age_in_ID = 0
	try:
		age_in_BD = int(time.strftime("%Y",time.localtime())) - int(row.birth_dt[0:4])
	except Exception, e:
		age_in_BD = 0
	if 18 <= age_in_ID <= 70:
		tem_tag[RID_CURRENT_AGE] = age_in_ID
	else:
		tem_tag[RID_CURRENT_AGE] = -1
	if tem_tag[RID_CURRENT_AGE] >= 18 and tem_tag[RID_CURRENT_AGE] < 25:
		tem_tag[RID_CURRENT_AGE_RANGE] = 1
	elif 25 <= tem_tag[RID_CURRENT_AGE] < 30: 
		tem_tag[RID_CURRENT_AGE_RANGE] = 2
	elif 30 <= tem_tag[RID_CURRENT_AGE] < 35:
		tem_tag[RID_CURRENT_AGE_RANGE] = 3
	elif 35 <= tem_tag[RID_CURRENT_AGE] < 40:
		tem_tag[RID_CURRENT_AGE_RANGE] = 4
	elif 40 <= tem_tag[RID_CURRENT_AGE] < 45:
		tem_tag[RID_CURRENT_AGE_RANGE] = 5
	elif 45 <= tem_tag[RID_CURRENT_AGE] < 50:
		tem_tag[RID_CURRENT_AGE_RANGE] = 6
	elif 50 <= tem_tag[RID_CURRENT_AGE] < 55:
		tem_tag[RID_CURRENT_AGE_RANGE] = 7
	elif 55 <= tem_tag[RID_CURRENT_AGE] < 60:
		tem_tag[RID_CURRENT_AGE_RANGE] = 8
	elif 60 <= tem_tag[RID_CURRENT_AGE] :
		tem_tag[RID_CURRENT_AGE_RANGE] = 9	
	else:
		tem_tag[RID_CURRENT_AGE_RANGE] = -1	
	#世代标签
	birth_year = time.localtime()[0] - tem_tag[RID_CURRENT_AGE]
	if 1990 <= birth_year < 1995:
		tem_tag[RID_CURRENT_GENERATION] = 1
	elif 1985 <= birth_year < 1990:
		tem_tag[RID_CURRENT_GENERATION] = 2
	elif 1980 <= birth_year < 1985:
		tem_tag[RID_CURRENT_GENERATION] = 3
	elif 1975 <= birth_year < 1980:
		tem_tag[RID_CURRENT_GENERATION] = 4
	elif 1970 <= birth_year < 1975:
		tem_tag[RID_CURRENT_GENERATION] = 5
	elif 1965 <= birth_year < 1970:
		tem_tag[RID_CURRENT_GENERATION] = 6
	elif 1960 <= birth_year < 1965:
		tem_tag[RID_CURRENT_GENERATION] = 7
	elif 1955 <= birth_year < 1960:
		tem_tag[RID_CURRENT_GENERATION] = 8
	elif 1995 <= birth_year < 2000 :
		tem_tag[RID_CURRENT_GENERATION] = 9
	else:
		tem_tag[RID_CURRENT_GENERATION] = -1		
	#星座标签 
	try:
		if abs(age_in_ID - age_in_BD) > 10:
			constellation_date = int(row.rid[10:14])
		elif row.birth_dt:
			constellation_date = int(row.birth_dt[5:7]+row.birth_dt[8:10])
	except Exception, e:
		constellation_date = -1
	if 120 <= constellation_date <= 218: 
		tem_tag[RID_CONSTELLATION] = 1
	elif 219 <= constellation_date <= 320:
		tem_tag[RID_CONSTELLATION] = 2
	elif 321 <= constellation_date <= 419:
		tem_tag[RID_CONSTELLATION] = 3
	elif 420 <= constellation_date <= 520:
		tem_tag[RID_CONSTELLATION] = 4
	elif 521 <= constellation_date <= 621:
		tem_tag[RID_CONSTELLATION] = 5
	elif 622 <= constellation_date <= 722:
		tem_tag[RID_CONSTELLATION] = 6
	elif 723 <= constellation_date <= 822:
		tem_tag[RID_CONSTELLATION] = 7
	elif 823 <= constellation_date <= 922:
		tem_tag[RID_CONSTELLATION] = 8
	elif 923 <= constellation_date <= 1023:
		tem_tag[RID_CONSTELLATION] = 9
	elif 1024 <= constellation_date <= 1122:
		tem_tag[RID_CONSTELLATION] = 10
	elif 1123 <= constellation_date <= 1221:
		tem_tag[RID_CONSTELLATION] = 11
	elif 1222 <= constellation_date <= 1231 or 101 <= constellation_date <= 119:
		tem_tag[RID_CONSTELLATION] = 12
	else:
		tem_tag[RID_CONSTELLATION] = -1
	#婚姻标签                                                                                             
	if row.marital_stat_cd == MARRIED:
		tem_tag[RID_MARITAL_STATUS] = 1 
	elif row.marital_stat_cd == SINGLE:
	 	tem_tag[RID_MARITAL_STATUS] = 2
	elif row.marital_stat_cd in ('null','NULL','Null',None):
		tem_tag[RID_MARITAL_STATUS] = -1
	else:
		tem_tag[RID_MARITAL_STATUS] = None
	#收入标签
	try:
		if MAPINCOME.has_key(row.income_range_cd):
			tem_tag[RID_MONTHLY_INCOME_RANGE] = MAPINCOME[row.income_range_cd]
		elif row.income_range_cd not in ('null','NULL','Null'):
			tem_tag[RID_MONTHLY_INCOME_RANGE] = -1
	except Exception, e:
		tem_tag[RID_MONTHLY_INCOME_RANGE] = None
	#学历标签
	try:
		if MAPEDUCATION.has_key(row.education_level):
			tem_tag[RID_EDUCATION] = MAPEDUCATION[row.education_level]
		elif row.education_level not in ('null','NULL','Null'):
			tem_tag[RID_EDUCATION] = -1 
	except Exception, e:
		tem_tag[RID_EDUCATION] = None
	#省份标签
	try:
		if MAPPROVINCE.has_key(row.state):
			tem_tag[RID_PERMANENT_PROVINCE] = MAPPROVINCE[row.state]
		elif row.state not in ('null','NULL','Null'):
			tem_tag[RID_PERMANENT_PROVINCE] = -1	
	except Exception, e:		
		tem_tag[RID_PERMANENT_PROVINCE] = None
	#城市标签&级别标签
	if row.city:
		tem_tag[RID_PERMANENT_CITY] = row.city
		if MAPCITY.has_key(tem_tag[RID_PERMANENT_CITY]):
			tem_tag[RID_PERMANENT_CITY_LEVEL] = MAPCITY[tem_tag[RID_PERMANENT_CITY]]
		else:
			tem_tag[RID_PERMANENT_CITY_LEVEL] = -1
	else:
		if row.addr_name:
			state_index = row.addr_name.find(state)
			district_index = row.addr_name.find(DISTRICT)
			city_index = row.addr_name.find(city)
			county_index = row.addr_name.find(COUNTY)
			if state_index != -1:
				if city_index != -1:
					tem_tag[RID_PERMANENT_CITY] = row.addr_name[state_index+1:city_index]
				elif county_index != -1:
					tem_tag[RID_PERMANENT_CITY] = row.addr_name[state_index+1:county_index]
				else:
					tem_tag[RID_PERMANENT_CITY] = -1
			elif district_index != -1:
				if city_index != -1:
					tem_tag[RID_PERMANENT_CITY] = row.addr_name[district_index+3:city_index]
				elif county_index != -1:
					tem_tag[RID_PERMANENT_CITY] = row.addr_name[district_index+3:county_index]
				else:
					tem_tag[RID_PERMANENT_CITY] = -1
			elif city_index != -1:
				tem_tag[RID_PERMANENT_CITY] = row.addr_name[0:county_index]
			else:
				tem_tag[RID_PERMANENT_CITY] = -1
			if tem_tag[RID_PERMANENT_CITY] != -1:
				if MAPCITY.has_key(tem_tag[RID_PERMANENT_CITY]):
					tem_tag[RID_PERMANENT_CITY_LEVEL] = MAPCITY[tem_tag[RID_PERMANENT_CITY]]
				else:
					tem_tag[RID_PERMANENT_CITY_LEVEL] = -1
			else:
				tem_tag[RID_PERMANENT_CITY_LEVEL] = -1
		else:
			tem_tag[RID_PERMANENT_CITY] = None
			tem_tag[RID_PERMANENT_CITY_LEVEL] = None
	#对省份标签是直辖市城市标签为空的标签修改：
	if tem_tag.get(RID_PERMANENT_PROVINCE) == 1:
		tem_tag[RID_PERMANENT_CITY] = u'北京市'
		tem_tag[RID_PERMANENT_CITY_LEVEL] = 1
	if tem_tag.get(RID_PERMANENT_PROVINCE) == 2:
		tem_tag[RID_PERMANENT_CITY] = u'天津市'
		tem_tag[RID_PERMANENT_CITY_LEVEL] = 1
	if tem_tag.get(RID_PERMANENT_PROVINCE) == 9:
		tem_tag[RID_PERMANENT_CITY] = u'上海市'
		tem_tag[RID_PERMANENT_CITY_LEVEL] = 1
	if tem_tag.get(RID_PERMANENT_PROVINCE) == 22:
		tem_tag[RID_PERMANENT_CITY] = u'重庆市'
		tem_tag[RID_PERMANENT_CITY_LEVEL] = 1
	#行业标签  
	if row.industry in ['null','NULL','Null',None]:
		tem_tag[RID_INDUSTRY] = None	                                                                                
	else:	
		if MAPINDUSTRY1.has_key(row.industry):
			tem_tag[RID_INDUSTRY] = MAPINDUSTRY1[row.industry]
		elif MAPINDUSTRY2.has_key(row.industry):
			tem_tag[RID_INDUSTRY] = MAPINDUSTRY2[row.industry]
		elif MAPINDUSTRY.has_key(row.industry):	
			tem_tag[RID_INDUSTRY] = MAPINDUSTRY[row.industry]
		else:
			tem_tag[RID_INDUSTRY] = -1
	#职业标签
	if row.job_title in ('null','NULL','Null',None):
		tem_tag[RID_JOB] = None
	else:
		if MAPJOB.has_key(row.job_title):
			tem_tag[RID_JOB] = MAPJOB[row.job_title]
		else: 
			tem_tag[RID_JOB] = -1
	#最近更新时间
	ret = (row.rid,(tem_tag,row.last_upd))
	return ret




# if __name__ == '__main__':
# 	row = [u'1@8172011315',u'2011-10-20 03:48:18.0',u'1-8PQT',u'2014-06-03 04:23:24.0',u'1-8PQT',u'24',u'0',u'NULL',u'677',u'1968-03-05 00:00:00.0',u'514',u'0',u'677',u'48',u'2014-06-03 04:23:24.0',u'2011-09-17 00:00:00.0',u'0',u'Y',u'0',u'2014-05-31 00:00:00.0',u'2011-09-12 00:00:00.0',u'9987',u'N',u'0',u'2011-09-17 08:46:23.0',u'N',u'NULL',u'0',u'0',u'3',u'1',u'NULL',u'n',u'60',u'0',u'39',u'N',u'1997',u'个人客户',u'江苏省扬州市江苏省仪征市新河路167号15幢205室',u'null',u'别克',u'钛金灰',u'英朗 GT 1.8L自动时尚型(皮)',u'英朗 GT GL',u'扬州市',u'愿意联系',u'1-6DC274',u'1-6DC1UC',u'王长明',u'ScriptingService_PreInvokeMethod',u'SJ1390',u'扬州广源汽车销售服务有限公司',u'上线',u'大本',u'null',u'null',u'null',u'null',u'上网/聊天/网络游戏,交际/派对/时尚活动,棋牌类娱乐,音乐,自驾游/汽车驾驶技巧培训',u'6000－9999元',u'日用品、耐用品、工业用品制造业',u'日用品、耐用品、工业用品制造业',u'品牌活动,用车相关,CRM服务',u'null',u'工程/制造/生产/营运/采购/物流',u'高层(部门总监、总经理等)',u'苏K-06N96',u'已婚',u'null',u'null',u'王长明',u'13805252090',u'13805252090',u'321081196803050310',u'别克六区-陈兆贵',u'零售',u'男',u'江苏省',u'英朗 GT',u'LSGPB54R2BS265030',u'null',u'RFS(别克6区)',u'211400',u'null',u'null',u'null',u'n',u'null',u'null',u'null',u'null',u'n',u'null',u'null',u'null',u'null',u'null',u'null',u'null',u'n',u'null',u'n',u'null',u'null',u'n',u'null',u'null',u'null']
# 	aa=getRIDTagFromCx_asset_tt(row)
# 	print aa
# 	#print "==============input row is : \n", row
	 # vin_tag = getVINTagsFromCx_asset_tt(row)
# 	# print "==============getVINTagsFromCx_asset_tt:", len(vin_tag) - 1
# 	# print vin_tag["tag_id"], vin_tag
# 	rid_tag = getRIDTagsFromCx_asset_tt(row)
# 	print "==============getRIDTagsFromCx_asset_tt:"
# 	print rid_tag["tag_id"], rid_tag

