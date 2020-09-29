# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 09:33:25 2020

@author: host
"""

import pymongo
import pandas as pd
import numpy as np
import datetime

is_True = False
CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
username = r'niewenjun'
password = r'niewenjun!@##@!'

def get_one_yesterday(day_min_time='00:00:00', day_max_time="23:59:59"):
    now_time = datetime.datetime.now()
    yesterday = now_time + datetime.timedelta(days=-1)
    
    now_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
    yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S')
    
    yesterday = yesterday.split(" ")[0]
    
    day_range = [yesterday + " " + day_min_time, yesterday + " " + day_max_time]
    return day_range


#调数仓
client = pymongo.MongoClient([CONN_ADDR])
client.dw_dws.authenticate(username, password)

db = client.dw_dws

apply_orders_collections = db['apply_orders']



#


