# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 09:10:53 2020

@author: host
"""

import pandas as pd
from sklearn.tree import DecisionTreeRegressor
import pymongo

input_data = r"C:\Users\host\Desktop\工作\商户监控\data\analysis"

CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
username = r'niewenjun'
password = r'niewenjun!@##@!'

#订单号
yzf_data = pd.read_csv(input_data + r"\售后报表_2020081209.csv", encoding='GBK')
yzf_data = yzf_data[yzf_data["是否为测试单"] == 0]
yzf_data["label"] = (yzf_data["逾期天数"]>30)

merge_data = yzf_data[["订单号", "label"]]

#调数仓
client = pymongo.MongoClient([CONN_ADDR])
client.dw_dws.authenticate(username, password)
db = client.dw_dws
apply_orders_collections = db['apply_orders']

searchRes = apply_orders_collections.find({"apply.created_at_str":{"$gte":'2019-01-01 00:00:00',\
        "$lte":'2019-12-31 23:59:59'}}, {"apply_no":1, "order_no":1, "user.mobile_md5":1, "user.base.age":1,\
        "user.base.idcard.no_md5":1,"user.base.gender":1,"_id":0, "apply.seller.id":1,\
        "apply.created_at_str":1, "credit.risk.content":1})

#解析
raw_data = pd.DataFrame(list(searchRes))

raw_data["mobile_md5"] = raw_data['user'].map(lambda x:x['mobile_md5'])
raw_data['age'] = raw_data["user"].map(lambda x:x['base']['age'])
raw_data['no_md5'] = raw_data['user'].map(lambda x:x["base"]["idcard"]['no_md5'])
raw_data['gender'] = raw_data['user'].map(lambda x:x["base"]["gender"])
raw_data['merchant_id'] = raw_data["apply"].map(lambda x:x["seller"]["id"])
raw_data['created_at_str'] = raw_data['apply'].map(lambda x:x['created_at_str'])

raw_data = raw_data.drop_duplicates(subset=["mobile_md5", "no_md5"])

#merge apply
data = raw_data[['apply_no', 'order_no' ,'merchant_id', 'age', 'gender']]
merge_data['订单号'] = merge_data['订单号'].map(lambda x:x[1:])
train_data = pd.merge(data, merge_data, left_on='order_no', right_on='订单号', how='left')

len(data) #880259
len(merge_data) #1572470
len(train_data) #880259

#同一个



