# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 10:55:26 2020

@author: host
翼支付商户监控_20200924
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


feature_list = ['order_no', 'apply_no', "user.mobile_md5", "user.base.age",\
                "user.base.idcard.no_md5", 'user.base.gender', 'apply.seller.id',\
                "apply.created_at_str", "user.base.address", "user.base.nation",\
                "user.company.salary", "user.base.marriage", "user.company.industry",\
                "user.edu.education", "apply.seller.name", "credit"]

class GET_DATA_AWAREHOUSE():
    def __init__(self, conn_addr=r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717',\
                 userName=r'niewenjun', passWord=r'niewenjun!@##@!'):
        self.conn_addr = conn_addr
        self.userName = userName
        self.passWord = passWord
    
    def init_authenticate(self):
        client = pymongo.MongoClient([self.conn_addr])
        client.dw_dws.authenticate(self.userName, self.passWord)
        self.client = client
        return self.client
        
    def get_database(self, database):
        self.dataBase = database
        self.client = self.init_authenticate()
        db = self.client[database]
        self.db = db
        return self.db
    
    def get_collection(self, database, collection):
        self.collection = collection
        db = self.get_database(database)
        data_collection = db[collection]
        return data_collection

class YZF_GET_DATA_AWAREHOUSE(GET_DATA_AWAREHOUSE):
    
    def get_transform_data(self, feature_list, database, collection,\
                           value_range=['2020-02-02 00:00:00', '2020-06-31 23:59:59'],\
                           exclude_feature="apply.created_at_str",\
                            mongo_id=False):
        self.feature_list = feature_list
        self.count = len(feature_list)
        self.is_mongo_id = mongo_id
        
        range_restriction = {exclude_feature:{"$gte":value_range[0],\
                                               "$lte":value_range[1]}}
        data_collection = self.get_collection(database, collection)
        extract_feature = {}
        for feature in feature_list:
            extract_feature[feature] = 1
        if mongo_id:
            extract_feature['_id'] = 1
        else:
            extract_feature['_id'] = 0
         
        print(range_restriction)
        print(extract_feature)
        searchRes = data_collection.find(range_restriction, extract_feature)
        
        raw_data = pd.DataFrame(list(searchRes))
        
        for feature in feature_list:
            self.__feature_split = feature.split(".")
            if len(self.__feature_split)==1:
                continue
            raw_data[self.__feature_split[-1]] = raw_data[self.__feature_split[0]].\
            map(self.transform_dict)
                
        self.columns = raw_data.columns
        raw_data = raw_data.drop_duplicates(subset=["mobile_md5", "no_md5"])
        return raw_data
    
    def transform_dict(self, value_dict):
        point_lenth = len(self.__feature_split)
        for i in range(1, point_lenth-1):
            value_dict = value_dict[self.__feature_split[i]]
        value = value_dict[self.__feature_split[point_lenth-1]]
        return value
    
day_range = get_one_yesterday()

yzf_data_warehouse = YZF_GET_DATA_AWAREHOUSE()  
raw_data = yzf_data_warehouse.get_transform_data(feature_list=feature_list,\
                            database='dw_dws', collection='apply_orders',\
                            value_range=day_range)

#日申请单量(A3),针对所有的商户，包括新老商户
def get_hit(value):
    flag = '未命中'
    if (value>13.5) & (value<=30.5):
        flag =  "预警值"
    if value>30.5:
        flag =  "风险值"
    return flag

temp_apply_count = raw_data["id"].value_counts().to_frame().reset_index()
temp_apply_count.columns = ["商户门店ID", "取值"]
temp_apply_count['特征'] = "日申请单(A3)"
temp_apply_count["命中"] = temp_apply_count["取值"].map(get_hit)
temp_apply_count['问题订单'] = "无"
temp_apply_count = temp_apply_count[['商户门店ID', '特征', '取值', '命中', '问题订单']]

#gender
def get_sex_table(df):
    flag ='未命中'
    df["is_bad_gender"] = df['gender'].map(lambda x:x==1)
    rate = df["is_bad_gender"].sum()/len(df)
    if (len(df)>5.5) & (len(df)<=10.5):
        if rate>0.73:
            flag = "预警值"
    if (len(df)<=30.5) & (len(df)>10.5):
        if rate>0.73:
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_gender]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["性别"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data
sex_table = raw_data.groupby("id")[["id", "apply_no", "gender"]].apply(get_sex_table)

#nation
def get_nation_table(df):
    flag ='未命中'
    df["is_bad_nation"] = df['nation'].map(lambda x:x==1)
    rate = df["is_bad_nation"].sum()/len(df)
    if (len(df)>=12) & (len(df)<=30.5):
        if rate>0.042:
            flag = "预警值"
    if (len(df)>30.5):
        if rate>0.042:
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_nation]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["民族"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data
raw_data['nation'] = raw_data['nation'].map(lambda x:x!='汉')
nation_table = raw_data.groupby("id")[["id", "apply_no", "nation"]].apply(get_nation_table)

#salary
def get_salary_table(df):
    flag ='未命中'
    
    rate_3 = len(df[df['salary'] == 3])/len(df)
    rate_4 = len(df[df['salary'] == 4])/len(df)
    
    df["is_bad_salary"] = df['salary'].map(lambda x:x==4)
    
    if (len(df)>5.5):
        if (rate_3>0.065) & (rate_4>0.211):
            flag = "预警值"
    if (len(df)>2.5) & (len(df)<=5.5):
        if (rate_4>0.367) & (rate_3>0.065):
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_salary]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["薪水区间"], "取值":[rate_4],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

salary_table = raw_data.groupby("id")[["id", "apply_no", "salary"]].apply(get_salary_table)

#day_apply merge age gender
def get_age_gender_table(df):
    flag = '未命中'
    
    df['is_bad_age_gender'] =(df['gender'] == 1) & (df['age'] < 23)
    
    rate_age = len(df[df['age'] < 23])/len(df)
    rate_gender = len(df[df['gender'] == 1])/len(df)
    
    if (len(df)>5.5) & (len(df)<=30.5):
        if (rate_age>0.207) & (rate_gender>0.73):
            flag='风险值'
    if (len(df)>5.5) & (len(df)<=30.5):
        if (rate_age<=0.207) & (rate_gender>0.73):
            flag='预警值'
    risk_apply_no = df['apply_no'][df.is_bad_age_gender]

    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["年龄性别"],\
                                "取值":[{"rate_age":rate_age, "rate_gender":rate_gender}],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data
age_gender_table = raw_data.groupby("id")[['id', 'apply_no', 'age', 'gender']].apply(get_age_gender_table)

#进单时间异常点
raw_data["进单时间"] = raw_data["created_at_str"].map(lambda x: int(x.split(r" ")[1].split(r":")[0]))

def get_time_table(df):
    flag = "未命中"
    df["early_warning_time"] = df['进单时间'].map(lambda x:x==23)
    df["risk_value_time"] = df["进单时间"].map(lambda x:(x>=0 and x<6))
    
    early_apply_no = df["apply_no"][df.early_warning_time].tolist()
    risk_apply_no = df["apply_no"][df.risk_value_time].tolist()
    if len(df)>1:
        if df["early_warning_time"].sum() > 0:
            flag = "预警值"
        if df["risk_value_time"].sum() > 0:
            flag = '风险值'
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["进单时间"], "取值":[-1],\
                "命中":[flag], "问题订单":[set(early_apply_no + risk_apply_no)]})
    return return_data

time_point_table = raw_data.groupby("id")[["id", "apply_no", "进单时间"]].apply(get_time_table)

#同盾数据
def transform(value):
    risk_value = value["risk"]
    if risk_value == []:
        return np.nan
    content = risk_value[0]
    return content['content']
raw_data['content'] = raw_data['credit'].map(transform)

#提出所有item_name(特征)
risk_items = []
def Content(x):
    if pd.isnull(x):
        return None
    else:
        x_tmp = x
        if 'risk_items' in x_tmp:
            item_names = [i["item_name"] for i in x_tmp["risk_items"]]
            global risk_items
            risk_items += item_names
        return None

raw_data["content"].apply(Content)
risk_items_all = list(set(risk_items))

#编码，if item_name存在为1, else 0
def get_risk_items(x_tmp):
    result = pd.Series([0 for i in risk_items_all])
    if type(x_tmp) == float:
        return result
    if ('success' not in x_tmp):
        return result
    if (x_tmp["success"] == False):
        return result
    else:
        item_names = [i["item_name"] for i in x_tmp["risk_items"]]
        result = [1 if i in item_names else 0 for i in risk_items_all]
    return pd.Series(result)
new_data = raw_data["content"].apply(get_risk_items)
new_data.columns = risk_items_all

#解析完过后，开始计算
def get_td_table(df):
    flag = "未命中"
    column = df.columns[-1]
    df["is_bad_td"] = df[column].map(lambda x:x>0)

    value = df[column].mean()
    if len(df)>3:
        if value>0.9:
            flag = "预警值"
        if value>0.98:
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_td]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":[column + "占比"], "取值":[value],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

columns = new_data.columns
columns = columns.to_list()
if "1年内申请人除本合作方在同盾全局没有申请借款记录" in columns:
    columns.remove("1年内申请人除本合作方在同盾全局没有申请借款记录")

td_data = pd.concat([raw_data[['id', "apply_no"]], new_data], axis=1)

concat_list = []
for column in columns:
    temp_td = td_data[['id', 'apply_no', column]].\
    groupby("id").apply(get_td_table)
    concat_list.append(temp_td)

td_table = pd.concat(concat_list)

#翼支付总和
yzf_merchant_table = pd.concat([temp_apply_count, sex_table, nation_table,\
                                salary_table, time_point_table, age_gender_table])

#风险等级
yzf_merchant_table['风险等级'] = yzf_merchant_table["命中"].map({"未命中":1, "预警值":2, "风险值":3})
important_yzf_merchant_table = yzf_merchant_table[yzf_merchant_table['命中'] != '未命中']
important_yzf_merchant_table = important_yzf_merchant_table[["商户门店ID", "特征", "取值", "命中", "风险等级", '问题订单']]

#订单列表
order_no_table = yzf_merchant_table[['问题订单', '风险等级']]
order_no_table = order_no_table[(order_no_table['风险等级'] != 1) & (order_no_table['问题订单']!='无')]

merge_order_no = []
for i in [2, 3]:
    temp_order_no = order_no_table[order_no_table['风险等级'] == i]
    temp_order_no = temp_order_no['问题订单'].to_list()
    proplem_order_set = set()
    for set_value in temp_order_no:
        proplem_order_set = (proplem_order_set | set_value)
    proplem_order_list = list(proplem_order_set)
    
    proplem_order = pd.DataFrame()
    proplem_order['问题订单列'] =  proplem_order_list
    proplem_order['风险等级'] = i
    merge_order_no.append(proplem_order.copy())
proplem_order = pd.concat([merge_order_no[0], merge_order_no[1]])
proplem_order = proplem_order.drop_duplicates(subset=['问题订单列'], keep='last')

write = pd.ExcelWriter(r"C:\Users\host\Desktop\工作\商户监控\yzf_merchant_monitoring.xlsx")
proplem_order.to_excel(excel_writer=write, sheet_name='问题订单汇总', index=False)
important_yzf_merchant_table.to_excel(excel_writer=write, sheet_name ="翼支付可疑商户", index=False)
yzf_merchant_table.to_excel(excel_writer=write, sheet_name ="翼支付原始数据", index=False)

write.save()
write.close()







