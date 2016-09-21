# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")

from mathartsys.supcap.tagFromMart.sys_datastore.datastore_mapkey import *
from mathartsys.supcap.tagFromMart.sys_datastore.datastore_union_table import * 
# from mathartsys.tagFromMart.supcap.sys_datastore.datastore_balance_order_table import *

#读取文件
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

#去重forrepair，claim，balance
def Deduplication(a,b):
	if a[-1]> b[-1]:
		return a
	else:
		return b

#doss取购车日期
def getOneDate(row):
	if row[1] != 'null':
		date = row[1]
	elif row[2] !='null':
		date = row[2]
	else:
		date = 'null'
	ret = (row[0],date)
	return ret

# #处理balance空日期
# def changeNullData(row):
# 	import datetime
# 	row = list(row)
# 	try:
# 		row[B_BALANCE_TIME] = datetime.datetime.strptime(row[B_BALANCE_TIME],"%Y-%m-%d %H:%M:%S.0")
# 	except:
# 		row[B_BALANCE_TIME] = datetime.datetime.today()
# 	return row

#取最近日期
def getLate(a,b):
	day_a = datetime.datetime.strptime(a,"%Y-%m-%d %H:%M:%S.0")
	day_b = datetime.datetime.strptime(b,"%Y-%m-%d %H:%M:%S.0")
	if day_a > day_b:
		return a
	else:
		return b

#late tag for repair
def getlatetag(row):
	today = datetime.datetime.today()
	inday =datetime.datetime.strptime(row[1],"%Y-%m-%d %H:%M:%S.0")
	if (today-inday).days <= 90:
		value = 1
	elif (today-inday).days <= 180:
		value = 2
	elif (today-inday).days <= 365:
		value = 3
	else:
		value = 4
	ret = (row[0],value,(today-inday).days)
	return ret

#repair tag function
def getTagsFromDatastore(row):
	import time
	inout_milediff=[]  #进出站里程差
	in_mile = []		#进站里程
	inin_milediff=[]	#进站里程差
	inwarranty_days = [0,]	#保内在站天数
	outwarranty_days = [0,]	#保外在站天数
	asc = [] 			#维修站
	acc_num = 0 		#事故次数
	acc_amount = 0 		#事故金额
	inwarranty_repair_num = 0 	#保内维修次数
	outwarranty_repair_num = 0  #保外维修次数
	inwarranty_repair_amount = 0 #保内维修金额
	outwarranty_repair_amount = 0 #保外维修金额
	paid_repair_num = 0  	 		#自费维修次数
	paid_repair_amount = 0 			#自费维修金额
	free_repair_num = 0 			#免费维修次数
	inwarranty_service_num = 0 		#保内保养次数
	outwarranty_service_num = 0 	#保外保养次数
	inwarranty_service_money = 0 	#保内保养金额
	outwarranty_service_money = 0  	#保外保养金额
	inwarranty_money = 0 			#保内总金额
	outwarranty_money = 0 			#保外总金额
	claim_num = 0 					#索赔次数
	claim_money = 0 				#索赔金额
	indate = []
	for info in row:
		info = info + (None,)		#索赔占位
		info = list(info)
		repair_mark = 0				#普通维修标记
		if not info[M_BALANCE_TIME] or info[M_BALANCE_TIME] in ('null','Null','NULL'):
			info[M_BALANCE_TIME] = info[M_START_TIME]
		if not info[M_IN_MILE] or info[M_IN_MILE] in ('null','Null','NULL'):
			info[M_IN_MILE] = 0
		if not info[M_OUT_MILE] or info[M_OUT_MILE] in ('null','Null','NULL'):
			info[M_OUT_MILE] = info[M_IN_MILE]
		if not info[M_PRINCING_DT]:
			info[M_PRINCING_DT] = "1900-01-01 00:00:00.0"
		info[M_PRINCING_DT]  = datetime.datetime.strptime(info[M_PRINCING_DT],"%Y-%m-%d %H:%M:%S.0")
		info[M_START_TIME]   = datetime.datetime.strptime(info[M_START_TIME],"%Y-%m-%d %H:%M:%S.0")
		info[M_TOTAL_AMOUNT] = int(round(float(info[M_TOTAL_AMOUNT])))
		info[M_IN_MILE] = int(round(float(info[M_IN_MILE])))
		info[M_OUT_MILE] = int(round(float(info[M_OUT_MILE])))
		try:
			info[M_BALANCE_TIME] = datetime.datetime.strptime(info[M_BALANCE_TIME],"%Y-%m-%d %H:%M:%S.0")
		except:
			print info[M_BALANCE_TIME]
			info[M_BALANCE_TIME] = info[M_START_TIME]
		if info[M_BRAND] == u'1' or info[M_BRAND] == u'2':
			if info[M_IN_MILE] <= 100000 and (info[M_START_TIME]-info[M_PRINCING_DT]).days <=365*3 and info[M_PRINCING_DT] >= datetime.datetime(2013,9,1):
				inwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				inwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					inwarranty_service_num += 1
					inwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					inwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						inwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						inwarranty_repair_num +=1
				#if info[M_PAY_AMOUNT]:
				#	claim_num += 1
				#	claim_money += info[M_PAY_AMOUNT]
			else:
				outwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				outwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					outwarranty_service_num += 1
					outwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					outwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						outwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						outwarranty_repair_num +=1
				#if info[M_PAY_AMOUNT]:
				#	claim_num += 1
				#	claim_money += info[M_PAY_AMOUNT]
		elif info[M_BRAND] == u'3':
			if (info[M_START_TIME]-info[M_PRINCING_DT]).days <=365*3 and (info[M_PRINCING_DT] >= datetime.datetime(2013,9,1) or info[M_IN_MILE] <= 100000):
				inwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				inwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					inwarranty_service_num += 1
					inwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					inwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						inwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						inwarranty_repair_num +=1
				#if info[M_PAY_AMOUNT]:
				#	claim_num += 1
				#	claim_money += info[M_PAY_AMOUNT]
			else:
				outwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				outwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					outwarranty_service_num += 1
					outwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					outwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						outwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						outwarranty_repair_num +=1
		else:
			if (info[M_START_TIME]-info[M_PRINCING_DT]).days <=365*3:
				inwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				inwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					inwarranty_service_num += 1
					inwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					inwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						inwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						inwarranty_repair_num +=1
				#if info[M_PAY_AMOUNT]:
				#	claim_num += 1
				#	claim_money += info[M_PAY_AMOUNT]
			else:
				outwarranty_days.append((info[M_BALANCE_TIME]-info[M_START_TIME]).days+1)
				in_mile.append(info[M_IN_MILE])
				outwarranty_money += info[M_TOTAL_AMOUNT]
				indate.append(info[M_START_TIME])
				if info[M_ASC] not in asc:
					asc.append(info[M_ASC])
				if info[M_OUT_MILE] == 0: #or ((info[M_OUT_MILE]-info[M_IN_MILE]) > 500)
					inout_milediff.append(0)
				else:
					inout_milediff.append(info[M_OUT_MILE]-info[M_IN_MILE])
				if u'保养' in info[M_REPAIR_TYPE]:
					outwarranty_service_num += 1
					outwarranty_service_money += info[M_TOTAL_AMOUNT]
					repair_mark += 1
				for key in ACCIDENTKEY:
					if key in info[M_REPAIR_TYPE]:
						acc_amount += info[M_TOTAL_AMOUNT]
						acc_num += 1
						repair_mark += 1
						break
				if repair_mark == 0:
					outwarranty_repair_amount += info[M_TOTAL_AMOUNT]
					paid_repair_amount += info[M_TOTAL_AMOUNT]
					if info[M_TOTAL_AMOUNT] == 0:
						free_repair_num += 1
						outwarranty_repair_num +=1
					else:
						paid_repair_num += 1
						outwarranty_repair_num +=1
				#if info[M_PAY_AMOUNT]:
				#	claim_num += 1
				#	claim_money += info[M_PAY_AMOUNT]
	in_mile.sort()
	for i in range(len(in_mile)-1):
		inin_milediff.append(in_mile[i+1]-in_mile[i])
	intimes            = acc_num+inwarranty_repair_num+outwarranty_repair_num+inwarranty_service_num+outwarranty_service_num
	maxinout_miles     = max(inout_milediff)    #最大进出站里程差
	if maxinout_miles < 0:
		maxinout_miles = 0
	try:
		maxinin_miles      = max(inin_milediff)	#最大进站里程差
		mininin_miles      = min(inin_milediff) #最小进站里程差
	except:
		maxinin_miles = None
		mininin_miles = None
	totaldaysin        = sum(inwarranty_days)+sum(outwarranty_days) #总在站天数
	inwarrantydaysin   = sum(inwarranty_days)						#保内在站总天数
	outwarrantydaysin  = sum(outwarranty_days)						#保外在站总天数
	maxdaysin          = max(max(inwarranty_days),max(outwarranty_days)) #最大在站天数
	inwarrantymaxdays  = max(inwarranty_days)							#保内单次最大在站天数
	outwarrantymaxdays = max(outwarranty_days)							#保外单次最大在站天数
	asc_num            = len(asc)										#维修站个数
	#acc_num															#事故次数
	#acc_amount   														#事故金额			
	totalrepair_num    = inwarranty_repair_num+outwarranty_repair_num	#维修总次数
	#inwarranty_repair_num												#保内维修次数
	#outwarranty_repair_num												#保外维修次数
	totalrepair_amount = inwarranty_repair_amount+outwarranty_repair_amount	#维修总金额
	#inwarranty_repair_amount											#保内维修金额
	#outwarranty_repair_amount											#保外维修金额
	#paid_repair_num													#自费维修次数
	#paid_repair_amount													#自费维修金额
	#free_repair_num													#免费维修次数
	totalsevice_num    = inwarranty_service_num+outwarranty_service_num #总保养次数
	#inwarranty_service_num												#保内保养次数
	#outwarranty_service_num											#保外保养次数
	totalsevice_amount = inwarranty_service_money+outwarranty_service_money#总保养金额
	#inwarranty_service_money												#保内保养金额
	#outwarranty_service_money												#保外保养金额
	total_amount = inwarranty_money+outwarranty_money                      #总金额
	#inwarranty_money                                                       #保内金额
	#outwarranty_money                                                      #保外金额
	#claim_num															 	#索赔次数
	#claim_money                                                            #索赔金额
	if info[M_PRINCING_DT] == datetime.datetime(1900,1,1,0,0):
		outwarrantydaysin = 0 
		outwarrantymaxdays = 0
		outwarranty_repair_num = 0
		outwarranty_repair_amount = 0
		outwarranty_service_num = 0
		outwarranty_service_money = 0
		outwarranty_money = 0
	ret = [intimes,maxinout_miles,maxinin_miles if maxinin_miles else None,mininin_miles if mininin_miles else None,totaldaysin,inwarrantydaysin,outwarrantydaysin,maxdaysin,inwarrantymaxdays,outwarrantymaxdays,asc_num,acc_num,acc_amount,totalrepair_num,inwarranty_repair_num,outwarranty_repair_num,totalrepair_amount,inwarranty_repair_amount,outwarranty_repair_amount,paid_repair_num,paid_repair_amount,free_repair_num,totalsevice_num,inwarranty_service_num,outwarranty_service_num,totalsevice_amount,inwarranty_service_money,outwarranty_service_money,total_amount,inwarranty_money,outwarranty_money]
	return ret

#claim tag function
#seqFunction
def seqFunciton(x,y):
	today = datetime.datetime.today()
	opendate = datetime.datetime.strptime(y[2],"%Y-%m-%d %H:%M:%S.0")
	if (today - opendate).days < 90:
		if y[0] in CLAIMWARRANTYLIST:
			tag = 1
			return (min(x[0],(today - opendate).days),min(x[1],tag),x[2],x[3],x[4]+1,x[5]+float(y[1]),x[6],x[7],x[8]+1,x[9]+float(y[1]),x[10],x[11],x[12]+1,x[13]+float(y[1]),x[14],x[15])
		elif y[0] in CLAIMGOODWILLLIST:
			tag = 1
			return (x[0],x[1],min(x[2],(today - opendate).days),min(x[3],tag),x[4],x[5],x[6]+1,x[7]+float(y[1]),x[8],x[9],x[10]+1,x[11]+float(y[1]),x[12],x[13],x[14]+1,x[15]+float(y[1]))
	elif (today - opendate).days < 180:
		if y[0] in CLAIMWARRANTYLIST:
			tag = 2
			return (min(x[0],(today - opendate).days),min(x[1],tag),x[2],x[3],x[4],x[5],x[6],x[7],x[8]+1,x[9]+float(y[1]),x[10],x[11],x[12]+1,x[13]+float(y[1]),x[14],x[15])
		elif y[0] in CLAIMGOODWILLLIST:
			tag = 2
			return (x[0],x[1],min(x[2],(today - opendate).days),min(x[3],tag),x[4],x[5],x[6],x[7],x[8],x[9],x[10]+1,x[11]+float(y[1]),x[12],x[13],x[14]+1,x[15]+float(y[1]))
	elif (today - opendate).days < 365:
		if y[0] in CLAIMWARRANTYLIST:
			tag = 3
			return (min(x[0],(today - opendate).days),min(x[1],tag),x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]+1,x[13]+float(y[1]),x[14],x[15])
		elif y[0] in CLAIMGOODWILLLIST:
			tag = 3
			return (x[0],x[1],min(x[2],(today - opendate).days),min(x[3],tag),x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14]+1,x[15]+float(y[1]))
	else:
		if y[0] in CLAIMWARRANTYLIST:
			tag = 4
			return (min(x[0],(today - opendate).days),min(x[1],tag),x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15])
		elif y[0] in CLAIMGOODWILLLIST:
			tag = 4
			return (x[0],x[1],min(x[2],(today - opendate).days),min(x[3],tag),x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15])

#combFunction
def combFunction(x,y):
	ret = (min(x[0],y[0]),min(x[1],y[1]),min(x[2],y[2]),min(x[3],y[3]),x[4]+y[4],x[5]+y[5],x[6]+y[6],x[7]+y[7], \
			x[8]+y[8],x[9]+y[9],x[10]+y[10],x[11]+y[11],x[12]+y[12],x[13]+y[13],x[14]+y[14],x[15]+y[15])
	return ret

#format for claim
def format(row):
	row = list(row)
	if row[0] == 999999:
		row[0] = 0
	else:
		row[0] = row[0]
	if row[1] == 999999:
		row[1] = 0
	else:
		row[1] = row[1]
	if row[2] == 999999:
		row[2] = 0
	else:
		row[2] = row[2]
	if row[3] == 999999:
		row[3] = 0
	else:
		row[3] = row[3]
	row[4] = int(round(row[4]))
	row[5] = int(round(row[5]))
	row[6] = int(round(row[6]))
	row[7] = int(round(row[7]))
	row[8] = int(round(row[8]))
	row[9] = int(round(row[9]))
	row[10] = int(round(row[10]))
	row[11] = int(round(row[11]))
	row[12] = int(round(row[12]))
	row[13] = int(round(row[13]))
	row[14] = int(round(row[14]))
	row[15] = int(round(row[15]))
	return row

	
if __name__ == '__main__':
	
	#row=['284232',u'5317134',u'6613031',u'NULL',u'2',u'780',u'2',u'2011-03-07 00:00:00.0',u'1',u'1000125',u'89791',u'4569',u'NULL',u'null',u'NULL',u'1',u'郑哥',u'13561733468',u'13561733468',u'null',u'null',u'0',u'2012-02-01 00:00:00.0',u'9999-12-31 00:00:00.0',u'1',u'2011-03-07 17:33:46.0',u'2012-02-01 10:35:16.0',u'2012-08-28 15:49:09.0',u'2012-08-28 15:49:09.0',u'DOSS']
	
	row=[(u'2',u'2010-5-1 0:00:00.0',u'123',u'2010-8-1 0:00:00.0',u'2010-8-5 0:00:00.0',2000,2005,500,u'保养'),(u'2',u'2010-5-1 0:00:00.0',u'124',u'2010-12-1 0:00:00.0',u'2010-12-1 0:00:00.0',3000,0,0,u'肇事'),(u'2',u'2010-5-1 0:00:00.0',u'123',u'2011-8-1 0:00:00.0',u'2011-9-1 0:00:00.0',5555,6000,500,u'aaa'),(u'2',u'2010-5-1 0:00:00.0',u'125',u'2014-8-1 0:00:00.0',u'2014-8-11 0:00:00.0',7000,7500,450,u'外保')]
	print getTagsFromDatastore(row)
