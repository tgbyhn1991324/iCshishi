# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")

"""
输入：inputdt：初始日期，interval:时间段
输出：初始日期开始，interval天的数据，rdd类型
功能描述：针对SequnceFile根据时间读取文件
"""	
def getSequnceFileBydt(sc, filepath,inputdt, interval):
	union_rdd = sc.parallelize([])
	for i in range(0,interval+1):
		dt = datetime.datetime.strptime(inputdt,"%Y%m%d") + datetime.timedelta(i)
		strdt =  datetime.datetime.strftime(dt,"%Y%m%d")
		subpath = filepath % (strdt,)
		try:
			cat_tf = sc.sequenceFile(subpath)
			cat_valid_rdd = cat_tf.map(lambda x: x[1].split("\x01")+[long(strdt),])
			union_rdd = union_rdd.union(cat_valid_rdd)
		except Exception, e:
			print subpath, "===does not exists==="
			continue
	return union_rdd

"""
输入：inputdt：初始日期，interval:时间段
输出：初始日期开始，interval天的数据，rdd类型
功能描述：针对TextFile根据时间读取文件
"""	
def getTextFileBydt(sc,filepath,inputdt,interval):
	union_rdd = sc.parallelize([])
	for i in range(0,interval+1):
		dt = datetime.datetime.strptime(inputdt,"%Y%m%d") + datetime.timedelta(i)
		strdt =  datetime.datetime.strftime(dt,"%Y%m%d")
		subpath = filepath % (strdt,)
		try:
			cat_tf = sc.textFile(subpath)
			cat_tf.first()
			cat_valid_rdd = cat_tf.map(lambda x: x.split("\x01")+[long(strdt),])
			union_rdd = union_rdd.union(cat_valid_rdd)
		except Exception, e:
			print subpath, "===does not exists==="
			continue
	return union_rdd

'''
作为reduceByKey的方法调用
功能描述:先比较dt字段，取dt大的数据
'''
def Deduplication(a,b):
	if a[-1]> b[-1]:
		return a
	else:
		return b

'''
功能描述:对supid key_key表赋予初始di，第一个元素为1，其他为0.9
'''
def getDefaultDi(row):
	a = len(row)
	di = 1.0
	ret = []
	for i in range(a):
		if row[i] not in (None,'null','Null','NULL',''):
			ret.append((row[i],di))
			di = 0.9
	return ret