# -*- coding: utf-8 -*-
"""
Created on Fri Aug  7 14:37:05 2020

@author: host
翼支付售后报表
"""
#手机号，身份证号，商户ID，申请日期，用户身份证归属地，手机号归属地，营业厅地址，年龄，
#性别，
import pandas as pd
import numpy as np
import pymongo
import datetime

now_time = datetime.datetime.now()
yesterday = now_time + datetime.timedelta(days=-1)
three_month_ago_time = now_time + datetime.timedelta(days=-91)

is_True = False
CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
username = r'niewenjun'
password = r'niewenjun!@##@!'
input_data = r"C:\Users\host\Desktop\工作\商户监控\data\monitoring"

now_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S')
three_month_ago_time = three_month_ago_time.strftime('%Y-%m-%d %H:%M:%S')

if is_True:
    #翼支付
    yzf_data = pd.read_csv(input_data + r"\售后报表_2020092809.csv", encoding='GBK',\
                           usecols=['是否为测试单', '办理完成时间', '商户门店ID'])
    yzf_data = yzf_data[yzf_data["是否为测试单"] == 0]
    
    def get_seconds_phone(df):
        flag = '未命中'
        seconds_phone = ((df["返费电话"] == "***********") | (df['用户电话'] == "***********"))
        risk_apply_no = df['订单号'][seconds_phone]
        seconds_phone_sum = seconds_phone.sum()
        rate = seconds_phone_sum/len(df)
    
        if (len(df)>2) * (rate>0.15):
            flag = '预警值'
        if (len(df)>2) * (rate>0.3):
            flag = '风险值'
        return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["二次放号"], "取值":[rate],\
                    "命中":[flag], "问题订单":[set(risk_apply_no)]})
        return return_data
    three_month_ago_data = yzf_data[yzf_data["办理完成时间"] <= three_month_ago_time]
    three_month_ago_and_user_data = three_month_ago_data[three_month_ago_data['用户电话'] == "***********"]
    merchant_list = list(set(three_month_ago_and_user_data['商户门店ID']))
    preparation_merchant_data = yzf_data[yzf_data["商户门店ID"].isin(merchant_list)]
    seconds_phone_table = preparation_merchant_data.groupby("商户门店ID")[['商户门店ID', '订单号', '用户电话', '返费电话']].apply(get_seconds_phone)
    seconds_phone_table["门店名称"] = seconds_phone_table['商户门店ID']

#调数仓
client = pymongo.MongoClient([CONN_ADDR])
client.dw_dws.authenticate(username, password)

db = client.dw_dws

apply_orders_collections = db['apply_orders']

#筛选
searchRes = apply_orders_collections.find({"apply.created_at_str":{"$gte":  yesterday.split(" ")[0] + ' 00:00:00',\
        "$lte": yesterday.split(" ")[0] + ' 23:59:59'}}, {'order_no':1, "apply_no":1,"user.mobile_md5":1, "user.base.age":1,\
         "user.base.idcard.no_md5":1,"user.base.gender":1,"_id":0, "apply.seller.id":1,\
         "apply.created_at_str":1, "credit.risk.content":1})

raw_data = pd.DataFrame(list(searchRes))

raw_data["mobile_md5"] = raw_data['user'].map(lambda x:x['mobile_md5'])
raw_data['age'] = raw_data["user"].map(lambda x:x['base']['age'])
raw_data['no_md5'] = raw_data['user'].map(lambda x:x["base"]["idcard"]['no_md5'])
raw_data['gender'] = raw_data['user'].map(lambda x:x["base"]["gender"])
raw_data['merchant_id'] = raw_data["apply"].map(lambda x:x["seller"]["id"])
raw_data['created_at_str'] = raw_data['apply'].map(lambda x:x['created_at_str'])

raw_data = raw_data.drop_duplicates(subset=["mobile_md5", "no_md5"])

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

time_point_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "进单时间"]].apply(get_time_table)

#age 文档中有修改
def get_age_table(df):
    flag = '未命中'
    df["is_bad_age"] = df['age'].map(lambda x:((x>60) | (x<24)))
    rate = df["is_bad_age"].sum()/len(df)
    if len(df)>3:
        if rate>=0.35:
            flag = "预警值"
        if rate>=0.8:
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_age]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["年龄"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data
age_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "age"]].apply(get_age_table)

#性别
def get_sex_table(df):
    flag ='未命中'
    df["is_bad_gender"] = df['gender'].map(lambda x:x==1)
    rate = df["is_bad_gender"].sum()/len(df)
    if len(df)>6:
        if rate>0.88:
            flag = "预警值"
        if rate>=1:
            flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_gender]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["性别"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data
sex_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "gender"]].apply(get_sex_table)

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

td_data = pd.concat([raw_data[['merchant_id', "apply_no"]], new_data], axis=1)

concat_list = []
for column in columns:
    temp_td = td_data[['merchant_id', 'apply_no', column]].\
    groupby("merchant_id").apply(get_td_table)
    concat_list.append(temp_td)

td_table = pd.concat(concat_list)

#总 concat
if is_True:
    yzf_merchant_table = pd.concat([time_point_table, age_table, sex_table, td_table, seconds_phone_table])
else:
    yzf_merchant_table = pd.concat([time_point_table, age_table, sex_table, td_table])
#风险等级
yzf_merchant_table['风险等级'] = yzf_merchant_table["命中"].map({"未命中":1, "预警值":2, "风险值":3})
important_yzf_merchant_table = yzf_merchant_table[yzf_merchant_table['命中'] != '未命中']
important_yzf_merchant_table = important_yzf_merchant_table[["商户门店ID", "特征", "取值", "命中", "风险等级", '问题订单']]
important_yzf_merchant_table.index = range(len(important_yzf_merchant_table))

proplem_order_set = set()
proplem_order = important_yzf_merchant_table[important_yzf_merchant_table['问题订单']!='没有']['问题订单']
proplem_order = proplem_order.to_list()

for set_value in proplem_order:
    proplem_order_set = (proplem_order_set | set_value)
proplem_order_list = list(proplem_order_set)

proplem_order = pd.DataFrame()
proplem_order['问题订单列'] = proplem_order_list
proplem_order['问题订单列'] = "'" + proplem_order['问题订单列']


write = pd.ExcelWriter(r"C:\Users\host\Desktop\工作\商户监控\yzf_merchant_monitoring.xlsx")
proplem_order.to_excel(excel_writer=write, sheet_name='问题订单汇总', index=False)
important_yzf_merchant_table.to_excel(excel_writer=write, sheet_name ="翼支付可疑商户", index=False)
yzf_merchant_table.to_excel(excel_writer=write, sheet_name ="翼支付原始数据", index=False)

write.save()
write.close()

##################数据分析###########################
yzf_merchant_table.index = range(len(yzf_merchant_table))
yzf_merchant_table[yzf_merchant_table["特征"] == "进单时间"]

sex_t = yzf_merchant_table[yzf_merchant_table["特征"] == "性别"]
sex_t = sex_t[sex_t["命中"] != "未命中"]












