# -*- coding: utf-8 -*-
from __future__ import division
import sys
import datetime
import math

if __name__ == '__main__':
	sys.path.insert(0, "../../../")
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("SuperID")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

from mathartsys.supcap.superid.superid_DiPR.dipath import *

Ti = 0.4
LD = 0.6
D = 0.5

def simplecal(row,sysmap,cmap):
	today = datetime.datetime.today()
	x = row.asDict()
	ret = {}
	ret['TIMES_HAT'] = sysmap[x['SYSTEM']] * x['DEFAULTDi'] * x['TIMES']/cmap[x['SYSTEM']]
	ret['DAY_HAT'] = sysmap[x['SYSTEM']] / ((today - datetime.datetime.strptime(x['LASTDATE'],"%Y-%m-%d")).days + 1)
	ret['SYS_VALUE'] = sysmap[x['SYSTEM']]
	ret['DEFAULTDi'] = x['DEFAULTDi']
	ret['FDi'] = ret['SYS_VALUE'] * ret['DEFAULTDi'] * (Ti * math.tanh(ret['TIMES_HAT']) + LD * ret['DAY_HAT'])
	return ret

def combin(row1,row2):
	ret = {}
	ret['SYS_VALUE'] = max(row1['SYS_VALUE'],row2['SYS_VALUE'])
	ret['TIMES_HAT'] = row1['TIMES_HAT'] + row2['TIMES_HAT']
	ret['DAY_HAT'] = max(row1['DAY_HAT'],row2['DAY_HAT'])
	ret['DEFAULTDi'] = max(row1['DEFAULTDi'],row2['DEFAULTDi'])
	ret['FDi'] = ret['SYS_VALUE'] * ret['DEFAULTDi'] * (Ti * math.tanh(ret['TIMES_HAT']) + LD * ret['DAY_HAT'])
	return ret

def fixDi(dict1):
	ret = {}
	if dict1['FDi'] > ((dict1['key1FDi'] + dict1['key2FDi'] - dict1['FDi'])/(dict1['key1Times'] + dict1['key2Times'] - 1)):
		D_new = D/(dict1['key1Times'] + dict1['key2Times'] - 1)
	elif dict1['FDi'] == ((dict1['key1FDi'] + dict1['key2FDi'] - dict1['FDi'])/(dict1['key1Times'] + dict1['key2Times'] - 1)):
		D_new = 0
	else:
		D_new = -1 * D/(dict1['key1Times'] + dict1['key2Times'] -1 )
	ret['FDi'] = dict1['FDi']
	ret['FDi_hat'] = dict1['FDi'] + D_new
	ret['DAY_HAT'] = dict1['DAY_HAT']
	return ret

def getfixDi(sqlContext,path,sysmap,cmap):
	key_key = sqlContext.parquetFile(path).map(lambda x: ((x[0],x[1]),simplecal(x,sysmap,cmap))) \
						.reduceByKey(combin)
	key_key.persist()
	key1FDi = key_key.map(lambda x: (x[0][0],(x[1]['FDi'],1))).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
	key2FDi = key_key.map(lambda x: (x[0][1],(x[1]['FDi'],1))).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
	key_key_fixDi = key_key.map(lambda x: (x[0][0],(x[0][1],x[1]['FDi'],x[1]['DAY_HAT']))).join(key1FDi) \
						 .map(lambda x: (x[1][0][0],(((x[0],)+x[1][0]),x[1][1]))).join(key2FDi) \
						 .map(lambda x: ((x[1][0][0][0],x[0]),{'FDi':x[1][0][0][2],'DAY_HAT':x[1][0][0][3],'key1FDi':x[1][0][1][0],'key1Times':x[1][0][1][1],'key2FDi':x[1][1][0],'key2Times':x[1][1][1]})) \
						 .map(lambda x: (x[0],fixDi(x[1])))
	return key_key_fixDi


def getDiOneToOne(keytable):
	key1_Di = keytable.map(lambda x: (x[0][0],(x[0][1],x[1]['FDi_hat'],x[1]['DAY_HAT']))).reduceByKey(O2Ogetmax) \
							.map(lambda x: ((x[0],x[1][0]),{'tag1':1}))
	key2_Di = keytable.map(lambda x: (x[0][1],(x[0][0],x[1]['FDi_hat'],x[1]['DAY_HAT']))).reduceByKey(O2Ogetmax) \
	 						.map(lambda x: ((x[1][0],x[0]),{'tag2':1}))
	keyfinal_Di = keytable.leftOuterJoin(key1_Di).map(lambda x: (x[0],dict(x[1][0],**x[1][1] or {'tag1':0}))) \
							.leftOuterJoin(key2_Di).map(lambda x: (x[0],dict(x[1][0],**x[1][1] or {'tag2':0}))) \
							.map(lambda x: (x[0]+(float(x[1]['FDi']),float(x[1]['tag1'] and x[1]['tag2']))))
	return keyfinal_Di

def getDiOneToMul(keytable,o2m):
	if o2m:
		key1_Di = keytable.map(lambda x: (x[0][0],(x[1]['DAY_HAT'],x[1]['FDi_hat']))).reduceByKey(O2Mgetmax) \
								.map(lambda x: (x[0],x[1][1]))
		keyfinal_Di = keytable.map(lambda x:(x[0][0],(x[0][1],x[1]['FDi'],x[1]['FDi_hat']))).leftOuterJoin(key1_Di) \
								.map(lambda x: (x[0],x[1][0][0],float(x[1][0][1]),float(max(x[1][0][2]/x[1][1],0))))
		return keyfinal_Di
	else:
		key1_Di = keytable.map(lambda x: ((x[0][1],x[0][0]),x[1])) \
								.map(lambda x: (x[0][0],(x[1]['DAY_HAT'],x[1]['FDi_hat']))).reduceByKey(O2Mgetmax) \
								.map(lambda x: (x[0],x[1][1]))
		keyfinal_Di = keytable.map(lambda x:(x[0][0],(x[0][1],x[1]['FDi'],x[1]['FDi_hat']))).leftOuterJoin(key1_Di) \
								.map(lambda x: (x[0],x[1][0][0],float(x[1][0][1]),float(max(x[1][0][2]/x[1][1],0))))
		return keyfinal_Di


def getDiMulToDiMul(keytable):
	key1_Di = keytable.map(lambda x: (x[0][0],x[0][1],float(x[1]['FDi']),float(min(max(x[1]['FDi_hat'],0),1))))
	return key1_Di

def O2Ogetmax(x,y):
	if x[1] > y[1]:
		return x
	if x[1] == y[1]:
		if x[2] < y[2]:
			return x
		else:
			return y
	else:
		return y

def O2Mgetmax(x,y):
	if x[-1] > y[-1]:
		return x
	if x[-1] == y[-1]:
		if x[-2] < y[-2]:
			return x
		else:
			return y
	else:
		return y

