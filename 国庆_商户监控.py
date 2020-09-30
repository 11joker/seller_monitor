# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 10:55:08 2020

@author: host
"""

import pandas as pd
from datetime import timedelta
import datetime

yzf_if = True
qyj_if =True

sc_if = True
qh_if = True

is_vip = False

input_folder = './data/国庆'
now_time = datetime.datetime.now()

yzf_afterloan_data = pd.read_csv(input_folder + r'/翼支付_售后报表_2020093011.csv',\
                                 encoding='GBK')
yzf_check_data = pd.read_csv(input_folder + r'/翼支付审核数据_20200930112223.csv',\
                             encoding='GBK')

qyj_afterloan_data = pd.read_csv(input_folder + r'/权益金_售后报表_2020093011.csv',\
                                 encoding='GBK')
qyj_check_data = pd.read_csv(input_folder + r'/和包审核数据_20200930112030.csv',\
                             encoding='GBK')

yzf_check_data = yzf_check_data.drop_duplicates(subset=["身份证唯一编码"])
qyj_check_data = qyj_check_data.drop_duplicates(subset=["身份证唯一编码"])

def get_one_yesterday(now_time, day_min_time='00:00:00', day_max_time="23:59:59", days=360):
    yesterday = now_time + timedelta(days=-1)
    before_time = yesterday + timedelta(days=-(days-1))
    
    yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S')
    before_time = before_time.strftime('%Y-%m-%d %H:%M:%S')
    
    yesterday = yesterday.split(" ")[0]
    before_time = before_time.split(" ")[0]
    
    day_range = [before_time + " " + day_min_time, yesterday + " " + day_max_time]
    return day_range

#申请单量
#翼支付
day_range_30 = get_one_yesterday(now_time, days=30)    

yzf_check_data['最后修改时间'] = yzf_check_data['最后修改时间'].map(lambda x:x.strip())
yzf_check_data['time'] = yzf_check_data['最后修改时间'].map(lambda x:x.split(" ")[0])    
yzf_check_data_30 = yzf_check_data[(yzf_check_data['最后修改时间'] >= day_range_30[0]) &\
                                   (yzf_check_data['最后修改时间']<=day_range_30[1])]

apply_count_temp = yzf_check_data_30.groupby(['门店id'])[['流水号']].count()
apply_count_temp.reset_index(inplace=True)

days = yzf_check_data_30.drop_duplicates(subset=['门店id', 'time'])
days = days.groupby(["门店id"])[['time']].count()
days.reset_index(inplace=True)

apply_count_yzf = pd.merge(apply_count_temp, days, on='门店id')
apply_count_yzf['日均进单'] = apply_count_yzf['流水号']/apply_count_yzf['time']
apply_count_yzf.drop(["time", "流水号"], axis=1, inplace=True)
day_mean_apply = apply_count_yzf.iloc[:]

day_range_1 = get_one_yesterday(now_time, days=1)
yzf_check_data_1 = yzf_check_data[(yzf_check_data['最后修改时间'] >= day_range_1[0]) & (yzf_check_data['最后修改时间']<=day_range_1[1])]
apply_count = yzf_check_data_1.groupby(["门店id"])[['流水号']].count()
apply_count.reset_index(inplace=True)
apply_count.columns = ['门店id', '当日进单量']

final_count = pd.merge(apply_count, day_mean_apply, on='门店id', how='left')

final_count['进单量波动'] = (final_count['当日进单量'] - final_count['日均进单'])/final_count['日均进单']

final_count.sort_values('进单量波动', ascending=False, inplace=True)
top_10_yzf = final_count.head(10)
top_10_yzf = top_10_yzf[['门店id', '进单量波动']]



#权益金
day_range_30 = get_one_yesterday(now_time, days=30)    

qyj_check_data['申请授信时间'] = qyj_check_data['申请授信时间'].map(lambda x:x.strip())
qyj_check_data['time'] = qyj_check_data['申请授信时间'].map(lambda x:x.split(" ")[0])    
qyj_check_data_30 = qyj_check_data[(qyj_check_data['申请授信时间'] >= day_range_30[0]) &\
                                   (qyj_check_data['申请授信时间']<=day_range_30[1])]

apply_count_temp = qyj_check_data_30.groupby(['门店id'])[['流水号']].count()
apply_count_temp.reset_index(inplace=True)

days = qyj_check_data_30.drop_duplicates(subset=['门店id', 'time'])
days = days.groupby(["门店id"])[['time']].count()
days.reset_index(inplace=True)

apply_count_qyj = pd.merge(apply_count_temp, days, on='门店id')
apply_count_qyj['日均进单'] = apply_count_qyj['流水号']/apply_count_qyj['time']
apply_count_qyj.drop(["time", "流水号"], axis=1, inplace=True)
day_mean_apply = apply_count_qyj.iloc[:]

day_range_1 = get_one_yesterday(now_time, days=1)
qyj_check_data_1 = qyj_check_data[(qyj_check_data['申请授信时间'] >= day_range_1[0]) & (qyj_check_data['申请授信时间']<=day_range_1[1])]
apply_count = qyj_check_data_1.groupby(["门店id"])[['流水号']].count()
apply_count.reset_index(inplace=True)
apply_count.columns = ['门店id', '当日进单量']

final_count = pd.merge(apply_count, day_mean_apply, on='门店id', how='left')

final_count['进单量波动'] = (final_count['当日进单量'] - final_count['日均进单'])/final_count['日均进单']

final_count.sort_values('进单量波动', ascending=False, inplace=True)
top_10_qyj = final_count.head(10)
top_10_qyj = top_10_qyj[['门店id', '进单量波动']]


#额度监控

yzf_afterloan_data['订单号'] = yzf_afterloan_data['订单号'].map(lambda x:x[1:])
yzf_check_data['订单号'] = yzf_check_data['订单号'].map(lambda x:x.strip())
data = pd.merge(yzf_afterloan_data[['订单号', '分期金额']], yzf_check_data[['订单号', '最终授信额度']], on='订单号', how='left')

data['未使用额度率'] = (data['最终授信额度'] - data['分期金额'])/data['最终授信额度']
not_use_amount_yzf = data[(data['未使用额度率'] < 0.1)]




