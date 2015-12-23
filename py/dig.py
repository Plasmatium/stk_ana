# -*- coding: UTF-8 -*-

import pandas as pd
from numpy import round
import pickle as pk
import zlib
import os
import time
from ipdb import *
from datetime import datetime, timedelta

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

idx0=idx1=idx2=idx3=idx4=idx5=idx6=''

#idx = ['代码', '开盘价', '最高价', '最低价', '收盘价', '成交量(手)', '成交额(元)'

class AdvDataFrame(pd.DataFrame):
	def __init__(self, *args, **kwargs):
		pd.DataFrame.__init__(self, *args, **kwargs)

	def dumpc(self, fdir='../convert/'):
		nc_data = pk.dumps(self)
		c_data = zlib.compress(nc_data)

		with open(fdir + self.name + '.cdt', 'wb') as f:
			pk.dump(c_data, f)

	def loadc(self, code, fdir = '../convert/'):
		fn = code + '.cdt'
		with open(fdir + fn, 'rb') as f:
			c_data = pk.load(f)

		nc_data = zlib.decompress(c_data)
		adf = pk.loads(nc_data)
		adf.name = code
		return adf

	def fetch_data(self, fn, fdir = '../'):
		df = pd.read_csv(fdir + fn + '.csv', encoding='gb2312', index_col='时间', parse_dates=True)
		self.__init__(index=df.index)
		self.name = df.代码.head(1).get_values()[0]

		idx = list(df.axes[1].get_values())	#获取各列名称；axes[0]是时间列

		# idx值为['代码', '开盘价', '最高价', '最低价', '收盘价', '成交量(手)', '成交额(元)', '复权系数']
		# 要做的是将各种价格除权

		for i in idx[:-1]:
			# 寻找价格数据
			if i in ['最高价', '最低价', '开盘价', '收盘价']:
				self[i] = round(df[i] / df.复权系数, 2)
			else:
				self[i] = df[i]

	def last30min(self):
		return self.tail(30)

	def nlast30min(self):
		return self.head(210)

	def get_from_date(self, date):
		return AdvDataFrame(self[self.index.date == date])

	#conditon, self contains all day
	def conditionv3(self, parent):
		l_30m = self.tail(30)
		cmo_rate = (l_30m[idx4]-l_30m[idx1])/l_30m[idx1]#close - open rate
		cmo_rr = cmo_rate.diff()
		return [cmo_rate, cmo_rr]

	def conditionv2(self, parent):
		l_30m = self.tail(30)
		highrise_set = l_30m[(l_30m[idx4]/l_30m[idx1]-1)>=0.005]
		if len(highrise_set) == 0:
			return []

		hr_headopen = highrise_set.iat[0, 1]
		hr_tailclose = highrise_set.iat[-1, 4]
		code = l_30m.iat[0,0]
		hr_rate = (hr_tailclose-hr_headopen)/hr_headopen
		if hr_rate < 0.03:
			#print('\tdate = %s, hr_rate = %f'%((l_30m.head(1).index.date)[0], hr_rate))
			return []

		hr_vol = highrise_set[idx5].sum()
		vol_rate = hr_vol/self[idx5].mean()
		if vol_rate < 20:
			#print('\tdate = %s, hr_vol = %f'%((l_30m.head(1).index.date)[0], vol_rate))
			return []

		ok_date = (l_30m.head(1).index.date)[0]
		
		#cond4
		tp5days = parent[parent.index.date > ok_date]
		tp1df = tp5days[0:240]
		tp2df = tp5days[240:480]
		tp3df = tp5days[480:720]
		tp4df = tp5days[720:960]


		tp0close = self.iat[-1, 4]
		tp1rate = tp1df[idx2].max()/tp0close-1
		if tp1rate < 0.05:
			return []

		tp2rate = tp2df[idx2].max()/tp0close-1
		tp3rate = tp3df[idx2].max()/tp0close-1
		tp4rate = tp4df[idx2].max()/tp0close-1
		t5rate = tp5days[idx2][0:960].max()/tp0close-1
		#tp1max = tp1df[idx2].max()
		#tp1min = tp1df[idx3].min()
		#tp1_hrate = (tp1max - tp0close)/tp0close
		#tp1_lrate = (tp1min - tp0close)/tp0close
		#tp5max = tp5df[idx2].max()
		#tp5min = tp5df[idx3].max()
		#tp5_hrate = (tp5max - tp0close)/tp0close
		#tp5_lrate = (tp5min - tp0close)/tp0close

		return [(code, ok_date, hr_rate, hr_vol, tp1rate, tp2rate, tp3rate, tp4rate, t5rate)]

	def condition(self):
		l_30m = self.last30min()
		nl_30m = self.nlast30min()
		min_l30m = l_30m[idx3].min()
		max_l30m = l_30m[idx2].max()
		min_tm = l_30m[l_30m[idx3]==min_l30m].index[0]
		max_tm = l_30m[l_30m[idx2]==max_l30m].index[0]



		#cond1
		if min_tm.time() > max_tm.time():
			#print(min_tm, max_tm)
			return []

		rate = (max_l30m - min_l30m)/min_l30m
		if rate < 0.03:	
			print(min_tm, 'rate=', rate)
			return []

		#cond2
		dmin = (max_tm-min_tm).total_seconds()/60+1
		dmin_rate = rate/dmin
		if dmin_rate < 0.01:
			print(min_tm, 'dmin_rate=', dmin_rate)
			return []

		#cond3
		#--------------------------------------------
		allday_avr_deal = self[idx5].sum()/len(self)
		sudden_deal_df = self[(self.index >= min_tm) & (self.index <= max_tm)]
		sudden_deal = sudden_deal_df[idx5].sum()
		deal_rate = sudden_deal/allday_avr_deal
		if deal_rate < 6:
			print(deal_rate)
			return []
		#--------------------------------------------

		return [l_30m.name, min_tm, rate, rate/((max_tm-min_tm).total_seconds()/60+1)]


		#q_nl_30m = self.nlast30min()[self.idx[5]].sum()
		#q_l_30m = self.last30min()[self.idx[5]].sum()
####################################################################################

def str2dt(str):
	return datetime.strptime(str, '%Y-%m-%d %H:%M:%S')

def loadc(code, fdir = '../convert/'):
	fn = code + '.cdt'
	with open(fdir + fn, 'rb') as f:
		c_data = pk.load(f)

	nc_data = zlib.decompress(c_data)
	ndf = AdvDataFrame(pk.loads(nc_data))
	ndf.name = code
	return ndf

def convert(startfile=None):
	fn_list = list(os.walk(top_dir))[0][2]
	idx = 0
	if startfile != None:
		idx = fn_list.index(startfile)

	for fn in fn_list[idx:]:
		time.sleep(0.001)
		
		adf = AdvDataFrame()
		adf.fetch_data(fn[:-4])
		adf.dumpc()

		print(adf.name + ' is converted')

def tester():
	adf = loadc('sz002491')
	d = adf.get_from_date(datetime(2015,11,4).date())
	return d

def run(df):
	g = df.groupby(lambda x: x.date)
	rslt = []

	for x in g:
		x = AdvDataFrame(x[1])
		tmprslt = x.conditionv2(df)
		rslt += tmprslt

	return rslt

def superFilter(div=1, part=0):
	fdir='../filter2/'
	fn_list = list(os.walk(top_dir + '/convert'))[0][2]
	fn_num = len(fn_list)
	single = fn_num/div
	start = int(part*single)
	end = int((part+1)*single)
	end = end>fn_num and fn_num or end

	title = '代码,\t 时间,\t 拉升涨幅,\t 拉升成交量(手),\t t+1涨幅,\t t+2涨幅,\t t+3涨幅,\t t+4涨幅,\t 6日最高涨幅\n'
	templete = '%s,\t %s'+'\t,%f'*7+'\n'
	
	f = open(fdir+'data.csv', 'w')
	f.writelines(title)
	for fn in fn_list[start:end]:
		time.sleep(0.001)
		lst = datetime.now()
		adf = loadc(fn[:-4])
		if adf[idx1].max() > 300:
			#print('%s is 指数'%fn)
			continue

		rslt = run(adf)
		if len(rslt) == 0:
			print('-------%s：无记录---------'%fn)
			continue
		for r in rslt:
			line = templete%r
			f.writelines(line)
		print('-------%s：记录%d条---------'%(fn, len(rslt)))
		print('passed:%f'%(datetime.now() - lst).total_seconds())
		print(fn, 'is completed\n')

	f.close()





####################### legacy code ##########################
def dumpc(ndf, fdir='../convert/', name=None):
	nc_data = pk.dumps(ndf)
	c_data = zlib.compress(nc_data)

	if name == None:
		name = ndf.name

	with open(fdir + name + '.cdt', 'wb') as f:
		pk.dump(c_data, f)

#fetch_data	参数：fn，文件名
#			返回：DataFrame
#			功能：读取csv文件，做数据整理，前复权
def fetch_data(fn):
	df = pd.read_csv(fn, encoding='gb2312', index_col='时间', parse_dates=True)
	ndf = AdvDataFrame(index=df.index)
	ndf.name = df.代码.head(1).get_values()[0]
	idx = list(df.axes[1].get_values())	#获取各列名称；axes[0]是时间列

	# idx值为['代码', '开盘价', '最高价', '最低价', '收盘价', '成交量(手)', '成交额(元)', '复权系数']
	# 要做的是将各种价格除权

	for i in idx[:-1]:
		# 寻找价格数据
		if i in ['最高价', '最低价', '开盘价', '收盘价']:
			ndf[i] = round(df[i] / df.复权系数, 2)
		else:
			ndf[i] = df[i]
	return ndf


#-------init--------
tmp = loadc('sz002491')
li = []
for s in tmp:
	li.append(s)
idx0,idx1,idx2,idx3,idx4, idx5, idx6 = li

