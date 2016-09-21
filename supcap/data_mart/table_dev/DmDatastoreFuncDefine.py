# -*- coding: utf-8 -*-
import sys
import datetime

if __name__ == '__main__':
	sys.path.insert(0, "../../../")

"""
输入：inputdt：初始日期，interval:时间段
输出：初始日期开始，interval天的数据，rdd类型
功能描述：针对textFile根据时间读取文件
"""	
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

#去重forrepair，claim，balance，作为reducebyKey的方法
def DeduplicationDt(a,b):
	chga = list(a)
	chgb = list(b)
	min_date = min(a[-3],b[-3])
	if a[-4] > b[-4]:
		chga[-3] = min_date
		return tuple(chga)
	else:
		chgb[-3] = min_date
		return tuple(chgb)


