# -*- coding: utf-8 -*-
import sys
import time
import datetime
from time import localtime,strptime
# from numpy import *

if __name__ == '__main__':
	sys.path.insert(0, "../../../")

from mathartsys.supcap.config.cac_config import *
from mathartsys.supcap.superid.phone_tags import *
from mathartsys.supcap.config.mapdefine_cac import *

def getTagsFromS_srv_req(row):
	tags={}
	days_temp = 99999	
	#初始值
	tags[PHONE_DAYS_INC_LATELY]=0
	tags[PHONE_DATE_INC_LATELY]=0
	tags[PHONE_TIMES_HARASS_LW1]=0
	tags[PHONE_TIMES_SUGGESTION_LW1]=0
	tags[PHONE_TIMES_RESUE_LW1]=0
	tags[PHONE_TIMES_COMPLAINT_LW1]=0
	tags[PHONE_TIMES_CONSULT_LW1]=0
	tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LW1]=0
	tags[PHONE_TIMES_INC_TOTAL_LW1]=0
	tags[PHONE_TIMES_HARASS_LM1]=0
	tags[PHONE_TIMES_SUGGESTION_LM1]=0
	tags[PHONE_TIMES_RESUE_LM1]=0
	tags[PHONE_TIMES_COMPLAINT_LM1]=0
	tags[PHONE_TIMES_CONSULT_LM1]=0
	tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LM1]=0
	tags[PHONE_TIMES_INC_TOTAL_LM1]=0
	tags[PHONE_TIMES_HARASS_LM3]=0
	tags[PHONE_TIMES_SUGGESTION_LM3]=0
	tags[PHONE_TIMES_RESUE_LM3]=0
	tags[PHONE_TIMES_COMPLAINT_LM3]=0
	tags[PHONE_TIMES_CONSULT_LM3]=0
	tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LM3]=0
	tags[PHONE_TIMES_INC_TOTAL_LM3]=0
	tags[PHONE_TIMES_HARASS_LT]=0
	tags[PHONE_TIMES_SUGGESTION_LT]=0
	tags[PHONE_TIMES_RESUE_LT]=0
	tags[PHONE_TIMES_COMPLAINT_LT]=0
	tags[PHONE_TIMES_CONSULT_LT]=0
	tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LT]=0
	tags[PHONE_TIMES_INC_TOTAL_LT]=0

	#总呼入次数
	tags[PHONE_TIMES_INC_TOTAL_LT] = len(row)
	for i in row:
		try:
			now = localtime()
		 	a1 = time.strptime(i[2],"%Y-%m-%d %H:%M:%S.0")
		 	dd = (datetime.datetime(*now[:3])-datetime.datetime(*a1[:3])).days
		except Exception, e:
			dd = -1
		if dd >= 0 and dd < days_temp:
			days_temp = dd

		sr_area = i[0]
		sr_sub_area = i[1]
		CALL_TYPE = 0
		if MAPCALL_TYPE.has_key(sr_area):
			CALL_TYPE = MAPCALL_TYPE[sr_area]
		elif sr_area == LOOSE_END:
			if sr_sub_area == EMERGENCY_RESCUE:
				CALL_TYPE = 4
			else:
				CALL_TYPE = 9
		elif sr_area == EXTERNAL:
			if sr_sub_area == HARASSING_CALL:
				CALL_TYPE = 3
			elif sr_sub_area == SERVICE_CONFLICT:
				CALL_TYPE = 5
			elif sr_sub_area == AFTER_SALE_SERVICE_CONSULTATION:
				CALL_TYPE = 1
			else :
				CALL_TYPE = 9
		elif sr_area == AFTER_SALE_SERVICE_CONSULTATION:
			if sr_sub_area == CONFLICT_PARTS or CONFLICT_CLAIMANT :
				CALL_TYPE = 5
			else:
				CALL_TYPE = 1
		elif sr_area == SALE_CONSULTATION:
			if sr_sub_area == TRY_DRIVE or DRIVE_LEADS or DRIVE_MSM:
				CALL_TYPE = 6	
			else:
				CALL_TYPE = 1
		elif sr_area == REPAIR:
			if sr_sub_area == EMERGENCY_RESCUE:
				CALL_TYPE = 4
		else:
			CALL_TYPE = 5
		#total标签
		if CALL_TYPE == 1:
			tags[PHONE_TIMES_CONSULT_LT] += 1
		if CALL_TYPE == 2:
			tags[PHONE_TIMES_SUGGESTION_LT] += 1
		if CALL_TYPE == 3:
			tags[PHONE_TIMES_HARASS_LT] += 1
		if CALL_TYPE == 4:
			tags[PHONE_TIMES_RESUE_LT] += 1
		if CALL_TYPE == 5:
			tags[PHONE_TIMES_COMPLAINT_LT] += 1
		if CALL_TYPE == 6:
			tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LT] += 1
		#近一星期标签
		if dd <= 7 and dd > 0:
			tags[PHONE_TIMES_INC_TOTAL_LW1] += 1
			if CALL_TYPE == 1:
				tags[PHONE_TIMES_CONSULT_LW1] += 1
			tags[PHONE_TIMES_CONSULT_LT] += 1
			if CALL_TYPE == 2:
				tags[PHONE_TIMES_SUGGESTION_LW1] += 1
			if CALL_TYPE == 3:
				tags[PHONE_TIMES_HARASS_LW1] += 1
			if CALL_TYPE == 4:
				tags[PHONE_TIMES_RESUE_LW1] += 1
			if CALL_TYPE == 5:
				tags[PHONE_TIMES_COMPLAINT_LW1] += 1
			if CALL_TYPE == 6:
				tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LW1] += 1
		#近一个月标签
		if dd <= 30 and dd > 0:
			tags[PHONE_TIMES_INC_TOTAL_LM1] += 1
			if CALL_TYPE == 1:
				tags[PHONE_TIMES_CONSULT_LM1] += 1
			if CALL_TYPE == 2:
				tags[PHONE_TIMES_SUGGESTION_LM1] += 1
			if CALL_TYPE == 3:
				tags[PHONE_TIMES_HARASS_LM1] += 1
			if CALL_TYPE == 4:
				tags[PHONE_TIMES_RESUE_LM1] += 1
			if CALL_TYPE == 5:
				tags[PHONE_TIMES_COMPLAINT_LM1] += 1
			if CALL_TYPE == 6:
				tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LM1] += 1
		#近三个月标签
		if dd <= 90 and dd > 0:
			tags[PHONE_TIMES_INC_TOTAL_LM3]  += 1
			if CALL_TYPE == 1:
				tags[PHONE_TIMES_CONSULT_LM3] += 1
			if CALL_TYPE == 2:
				tags[PHONE_TIMES_SUGGESTION_LM3] += 1
			if CALL_TYPE == 3:
				tags[PHONE_TIMES_HARASS_LM3] += 1
			if CALL_TYPE == 4:
				tags[PHONE_TIMES_RESUE_LM3] += 1
			if CALL_TYPE == 5:
				tags[PHONE_TIMES_COMPLAINT_LM3] += 1
			if CALL_TYPE == 6:
				tags[PHONE_TIMES_CONSULT_TEST_DRIVING_LM3] += 1

	#最近一次呼入时间
	if 0 <= days_temp < 99999:
		tags[PHONE_DAYS_INC_LATELY] = days_temp
		if days_temp <= 30:
			tags[PHONE_DATE_INC_LATELY] = 1
		elif 90 <= days_temp <= 180:
			tags[PHONE_DATE_INC_LATELY] = 2
		elif 180 <= days_temp <= 365:
			tags[PHONE_DATE_INC_LATELY] = 3
		else:
			tags[PHONE_DATE_INC_LATELY] = 4
	return tags

# if __name__ == '__main__':
# 	row = [(VIHICLE_CONSULTATION,u'null',u'2015-05-20 01:41:12.0'),(SALE_CONSULTATION,TRY_DRIVE,u'2015-05-20 01:41:12.0'), \
# 	 (VIHICLE_CONSULTATION,u'null',u'2015-05-11 01:41:12.0'),(VIHICLE_CONSULTATION,u'null',u'2015-05-21 01:41:12.0'),(VIHICLE_CONSULTATION,u'null',u'2015-05-11 01:41:12.0'),(VIHICLE_CONSULTATION,u'null',u'2015-05-11 01:41:12.0')]
# 	phone_tag = getTagsFromS_srv_req(row)
# 	print phone_tag
