# -*- coding: utf-8 -*-
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/hdusmsm01/supcap/src/com')
import MySQLdb

if __name__ == '__main__':
	from pyspark import SparkConf, SparkContext
	conf = SparkConf().setAppName("PySpark_SUPCAP_carTypeUpdate")
	sc = SparkContext(conf=conf)
	from pyspark.sql import *
	sqlContext = SQLContext(sc)
	try:
		from pyspark.sql.types import *
	except:
		from pyspark.sql import *

def autoCarTypeUpdate():
	#hdfs上车型对应关系结构
	series_value_schema = StructType([StructField("tag",  MapType(StringType(), IntegerType(),False), False)])
	#获取最新的车型对应关系
	carBrandModelArray = sqlContext.parquetFile('/project/supcap/iinsight/data_mart/dm_table/dm_vehicle_info_a/') \
						.map(lambda x: (x.brand_name,x.car_model_name)).distinct().filter(lambda x: x[1] != None).collect()
	# carBrandModelArray = [(u'2', u'\u65af\u5e15\u53ef'), (u'1', u'\u82f1\u6717 GT'), (u'2', u'\u6c83\u84dd\u8fbe'), (u'3', u'SRX'), (u'3', u'\u51ef\u96f7\u5fb7'), (u'2', u'\u7231\u552f\u6b27\u4e09\u53a2'), (u'2', u'\u8fc8\u9510\u5b9d'), (u'1', u'\u541b\u5a01'), (u'2', u'\u4e50\u98ce'), (u'1', u'\u65b0\u541b\u8d8a'), (u'2', u'\u7ecf\u5178\u79d1\u9c81\u5179'), (u'1', u'\u65b0\u541b\u5a01'), (u'1', u'\u6602\u79d1\u5a01'), (u'3', u'CTS-V Coupe'), (u'1', u'\u6602\u79d1\u96f7'), (u'2', u'\u65b0\u8d5b\u6b27\u4e09\u53a2'), (u'2', u'\u8d5b\u6b27SPRINGO\u7eaf\u7535\u52a8\u8f66'), (u'1', u'\u6797\u836b\u5927\u9053'), (u'2', u'\u521b\u9177'), (u'1', u'\u51ef\u8d8a'), (u'3', u'CTS'), (u'2', u'\u4e50\u98ce RV'), (u'2', u'\u79d1\u9c81\u5179\u4e09\u53a2'), (u'2', u'\u7231\u552f\u6b27\u4e24\u53a2'), (u'2', u'\u79d1\u8fc8\u7f57'), (u'2', u'\u5168\u65b0\u79d1\u9c81\u5179'), (u'2', u'\u79d1\u5e15\u5947'), (u'2', u'\u4e50\u9a8b'), (u'1', u'\u5a01\u6717\u4e24\u53a2'), (u'3', u'XLR'), (u'1', u'\u5a01\u6717'), (u'2', u'\u79d1\u9c81\u5179\u6380\u80cc\u8f66'), (u'3', u'CT6'), (u'1', u'\u8363\u5fa1'), (u'3', u'NG CTS'), (u'1', u'\u522b\u514b\u8d5b\u6b27'), (u'2', u'\u79d1\u5e15\u5947 CKD'), (u'3', u'CTS Coupe'), (u'1', u'\u541b\u8d8a'), (u'2', u'\u65b0\u8d5b\u6b27\u4e24\u53a2'), (u'3', u'\u8d5b\u5a01'), (u'3', u'ATS'), (u'1', u'\u5168\u65b0\u541b\u5a01'), (u'3', u'XTS'), (u'1', u'GL8\u5546\u52a1\u8f66'), (u'1', u'\u82f1\u6717 XT'), (u'2', u'\u8d5b\u6b273'), (u'1', u'\u5168\u65b0\u541b\u8d8a'), (u'3', u'ATS-L'), (u'2', u'\u5168\u65b0\u8fc8\u9510\u5b9d'), (u'2', u'\u666f\u7a0b'), (u'2', u'\u8d5b\u6b27'), (u'1', u'\u5168\u65b0\u82f1\u6717'), (u'1', u'\u6602\u79d1\u62c9')]
	mapmodel = sqlContext.parquetFile('/project/supcap/iinsight/data_mart/lookup_table/tag_value') \
					.map(lambda x: x.tag).reduce(lambda x,y:dict(x,**y))
	#获取车型代码值的最大值
	modelValues_max = max(mapmodel.values())
	'''
	将系统里新增的车型数据插入mysql表中
	'''
	filter_key = None
	filter_value = None
	brand = None
	order = min(mapmodel.values())+1
	tag = 0 #标记是否更新order值
	for carBrandModel in carBrandModelArray:
		if carBrandModel[1] not in mapmodel.keys():
			mapmodel[carBrandModel[1]] = modelValues_max + 1
			filter_key = carBrandModel[1]
			filter_value = mapmodel[carBrandModel[1]]
			if carBrandModel[0] == '1':
				brand = u'别克'
			elif carBrandModel[0] == '2':
				brand = u'雪佛兰'
			elif carBrandModel[0] == '3':
				brand = u'凯迪拉克'
			else:
				brand = u'其他'
			row_tag = ('车型','VIN_CAR_MODEL',filter_key,filter_value,'车辆属性','car',0)
			row_brandmodeltype = [brand,filter_key,'NULL']
			#连接数据库
			# conn=MySQLdb.connect(host='10.203.98.7',user='root',passwd='',db='supcap',charset='utf8') #web测试环境
			conn=MySQLdb.connect(host='10.203.44.183',user='supcap',passwd='',db='supcap',charset='utf8') #web生产环境
			#使用cursor()方法获取操作游标
			cur = conn.cursor()
			cur.execute("insert into DataConfig_tag (`tag_cn`,`tag_en`,`filter_key`,`filter_value`,`category_cn`,`category_en`,`order`) values (%s,%s,%s,%s,%s,%s,%s)", row_tag)
			cur.execute("insert into DataConfig_brandmodeltype (`brand`,`model`,`type`) values(%s,%s,%s)", row_brandmodeltype)
			## 提交到数据库执行
			conn.commit()
			modelValues_max += 1
			tag = 1
	if tag == 1:
		cur.execute("update DataConfig_tag  set `order` =  `order` + 1 where tag_cn = '车型'")
		conn.commit()
		mapmodel_arr = []
		for i in mapmodel.keys():
			temp=[{i:mapmodel[i]},]
			mapmodel_arr.append(temp)
		series_rows = sc.parallelize(mapmodel_arr)
		series_rows_schemardd = sqlContext.applySchema(series_rows, series_value_schema).coalesce(5)
		series_pf = sqlContext.parquetFile('/project/supcap/iinsight/data_mart/lookup_table/tag_value')
		series_pf.registerTempTable("tag_value")
		series_rows_schemardd.insertInto("tag_value",overwrite=True)
		# 关闭数据库连接
		cur.close()
		conn.close()

if __name__ == '__main__':
	autoCarTypeUpdate()

# MAPMODEL = {
# u'E17':                       1,
# u'乐骋':                      2,
# u'乐风':                      3,
# u'科帕奇':                    4,
# u'赛欧':                      5,
# u'科鲁兹三厢':                6,
# u'新赛欧两厢':                7,
# u'斯帕可':                    8,
# u'爱唯欧两厢':                9,
# u'景程':                      10,
# u'新赛欧三厢':                11,
# u'爱唯欧三厢':                12,
# u'科迈罗':                    13,
# u'迈锐宝':                    14,
# u'沃蓝达':                    15,
# u'科帕奇 CKD':                16,
# u'赛欧SPRINGO纯电动车':       17,
# u'科鲁兹掀背车':              18,
# u'创酷':                      19,
# u'经典科鲁兹':                20,
# u'全新科鲁兹':                21,
# u'赛欧3':                     22,
# u'赛欧RV':                    23,
# u'君威':                      24,
# u'林荫大道':                  25,
# u'荣御':                      26,
# u'新君威':                    27,
# u'别克赛欧':                  28,
# u'昂科雷':                    29,
# u'新君越':                    30,
# u'英朗 XT':                   31,
# u'GL8商务车':                 32,
# u'英朗 GT':                   33,
# u'凯越':                      34,
# u'昂科拉':                    35,
# u'君越':                      36,
# u'全新君越':                  37,
# u'全新君威':                  38,
# u'昂科威':                    39,
# u'全新英朗':                  40,
# u'威朗':                      41,
# u'Omega':                     42,
# u'赛威':                      43,
# u'凯雷德':                    44,
# u'XTS':                       45,
# u'CTS Coupe':                 46,
# u'CTS':                       47,
# u'CTS-V Coupe':               48,
# u'SRX':                       49,
# u'ATS':                       50,
# u'NG CTS':                    51,
# u'XLR':                       52,
# u'ATS-L':                     53
# }

# carTypeArray = ['ATS',
# u'ATS-L',
# u'CT6',
# u'CTS',
# u'CTS Coupe',
# u'CTS-V Coupe',
# u'D2JC',
# u'D2LC',
# u'E17',
# u'E2SC',
# u'GL8商务车',
# u'K216',
# u'NG CTS',
# u'SRX',
# u'XLR',
# u'XTS',
# u'爱唯欧两厢',
# u'爱唯欧三厢',
# u'昂科拉',
# u'昂科雷',
# u'昂科威',
# u'别克赛欧',
# u'创酷',
# u'经典科鲁兹',
# u'景程',
# u'君威',
# u'君越',
# u'凯雷德',
# u'凯越',
# u'科鲁兹三厢',
# u'科鲁兹掀背车',
# u'科迈罗',
# u'科帕奇',
# u'科帕奇 CKD',
# u'乐骋',
# u'乐风',
# u'乐风 RV',
# u'林荫大道',
# u'迈锐宝',
# u'全新君威',
# u'全新君越',
# u'全新科鲁兹',
# u'全新迈锐宝',
# u'全新英朗',
# u'荣御',
# u'赛欧',
# u'赛欧3',
# u'赛欧SPRINGO纯电动车',
# u'赛威',
# u'斯帕可',
# u'威朗',
# u'威朗两厢',
# u'沃蓝达',
# u'新君威',
# u'新君越',
# u'新赛欧两厢',
# u'新赛欧三厢',
# u'英朗 GT',
# u'英朗 XT']
