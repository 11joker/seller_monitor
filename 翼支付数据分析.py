# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 18:28:27 2020

@author: host
翼支付售后报表全部
"""

import pandas as pd
import pymongo
import datetime
from matplotlib import pyplot as plt
import seaborn as sns
import toad

day = 20

CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
username = r'niewenjun'
password = r'niewenjun!@##@!'
input_data = r"C:\Users\host\Desktop\工作\商户监控\data\analysis"

start_time = '2019-01-01 00:00:00'
end_time = '2019-12-31 23:59:59'

plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

now_time = datetime.datetime(2019, 12, 31, 9, 42, 40, 159409)
time_list = []
for i in range(day):
    time_list.append(now_time + datetime.timedelta(days=-(i+1)))

three_month_ago_time = now_time + datetime.timedelta(days=-91)

for i in range(len(time_list)):
    time_list[i] = time_list[i].strftime('%Y-%m-%d %H:%M:%S')


#翼支付
yzf_data = pd.read_csv(input_data + r"\售后报表_2020081209.csv", encoding='GBK')
yzf_data = yzf_data[yzf_data["是否为测试单"] == 0]

yzf_data = yzf_data[(yzf_data["办理完成时间"] > start_time) & (yzf_data['办理完成时间']<end_time)]

#调数仓
client = pymongo.MongoClient([CONN_ADDR])
client.dw_dws.authenticate(username, password)

db = client.dw_dws

apply_orders_collections = db['apply_orders']

#筛选
searchRes_list = []
for i in range(len(time_list)):
    time = time_list[i]
    searchRes = apply_orders_collections.find({"apply.created_at_str":{"$gte":  time.split(" ")[0] + ' 00:00:00',\
            "$lte": time.split(" ")[0] + ' 23:59:59'}}, {"order_no":1, "apply_no":1,"user.mobile_md5":1, "user.base.age":1,\
             "user.base.idcard.no_md5":1,"user.base.gender":1,"_id":0, "apply.seller.id":1,\
             "apply.created_at_str":1, "credit.risk.content":1})
    searchRes_list.append(searchRes)

raw_data_list = []
for i in range(len(searchRes_list)):
    searchRes = searchRes_list[i]
    raw_data = pd.DataFrame(list(searchRes))
    raw_data_list.append(raw_data)

data_list = []
for i in range(len(raw_data_list)):
    if i == 0:
        continue
    raw_data = raw_data_list[i]
    raw_data["mobile_md5"] = raw_data['user'].map(lambda x:x['mobile_md5'])
    raw_data['age'] = raw_data["user"].map(lambda x:x['base']['age'])
    raw_data['no_md5'] = raw_data['user'].map(lambda x:x["base"]["idcard"]['no_md5'])
    raw_data['gender'] = raw_data['user'].map(lambda x:x["base"]["gender"])
    raw_data['merchant_id'] = raw_data["apply"].map(lambda x:x["seller"]["id"])
    raw_data['created_at_str'] = raw_data['apply'].map(lambda x:x['created_at_str'])
    raw_data = raw_data.drop_duplicates(subset=["mobile_md5", "no_md5"])
    data_list.append(raw_data.copy())

#-------计算每天平均申请单量和---------------
apply_sum = 0
merchant_sum = 0
for i in range(len(data_list)):
    raw_data = data_list[i]
    today_sum = len(raw_data)
    apply_sum += today_sum
    merchant_sum += len(set(raw_data['merchant_id']))

print("每日平均进单数量:", apply_sum/len(data_list))
print("每日平均进单商户数量:", merchant_sum/len(data_list))
print("多少日进行监控:", day)
"""
按照15天内的数据进行计算
每日平均进单数量: 3560.684210526316
每日平均进单商户数量: 2277.2105263157896
"""
#---------------age--------------------
#坏客户年龄:得出的年龄在<24,高风险
temp = yzf_data[["办理完成时间", "年龄", "逾期天数"]]
temp = temp[(temp["办理完成时间"]>start_time) & (temp["办理完成时间"]<end_time)]
age_bad_people = temp[["年龄", "逾期天数"]]
age_bad_people["label"] = age_bad_people['逾期天数']>30
combiner = toad.transform.Combiner()
combiner.fit(age_bad_people[["年龄", "label"]], y='label',method='chi', n_bins=5)
combiner.rules
age_combiner = combiner.transform(age_bad_people[["年龄", "label"]])
age_combiner["年龄"] = age_combiner['年龄'].map({0:"(-,23)", 1:"[23, 30)", 2:"[30,49)", 3:"[49,53)", 4:"[53,+)"})
age_combiner.groupby("年龄")["label"].mean().plot(kind='bar', rot=360)
plt.title("各年龄段逾期率")
plt.ylabel("逾期率")
plt.show()
plt.savefig(input_data + r'\age_overdue.png')
"""
[33,50)     299913
[24, 33)    152689
[50,56)     109641
(-,24)       26058
[56,+)        6699
"""

def get_age_table(df):
    flag = '未命中'
    df["is_bad_age"] = df['age'].map(lambda x:((x>60) | (x<23)))
    rate = df["is_bad_age"].sum()/len(df)
    if len(df)>3:
        flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_age]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["年龄"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

age_list = []
for i in range(len(data_list)):
    raw_data = data_list[i]
    age_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "age"]].apply(get_age_table)
    age_list.append(age_table)

age_table = pd.concat(age_list)
age_table_da = age_table[age_table["命中"] == "风险值"]
#筛除商户日进单<=2的商户，4天的商户数据
plt.title("商户监控-年龄异常(大于60小于24)箱线图")
sns.boxplot(age_table_da["取值"])
plt.xlabel("商户年龄异常占比")
plt.show()
plt.savefig(input_data + r'\bad_age_box.png')

#------------------sex------
#性别
yzf_data["性别"] = yzf_data["身份证"].map(lambda x:(int(x[-2])%2) if (x[-2]>="0")*(x[-2]<="9") else -1)

temp = yzf_data[["办理完成时间", "性别", "逾期天数"]]
temp = temp[(temp["办理完成时间"]>start_time) & (temp["办理完成时间"]<end_time)]
age_bad_people = temp[["性别", "逾期天数"]]

age_bad_people["label"] = age_bad_people['逾期天数']>30
combiner = toad.transform.Combiner()
combiner.fit(age_bad_people[["性别", "label"]], y='label',method='chi', n_bins=2)
combiner.rules
age_combiner = combiner.transform(age_bad_people[["性别", "label"]])
age_combiner.groupby("性别")["label"].mean().plot(kind='bar', rot=360)
plt.title("性别逾期率")
plt.ylabel("逾期率")
plt.show()
plt.savefig(input_data + r"\sex_overdue.png")
"""
1    370315
0    224685
"""

#筛出<=5的
def get_sex_table(df):
    flag ='未命中'
    df["is_bad_gender"] = df['gender'].map(lambda x:x==1)
    rate = df["is_bad_gender"].sum()/len(df)
    if len(df)>8:
        flag = '风险值'
    risk_apply_no = df["apply_no"][df.is_bad_gender]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["性别"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

sex_list = []
for i in range(len(data_list)):
    raw_data = data_list[i]
    sex_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "gender"]].apply(get_sex_table)
    sex_list.append(sex_table)

sex_table = pd.concat(sex_list)
sex_table_da = sex_table[sex_table["命中"] == "风险值"]
#筛除商户日进单<=2的商户，4天的商户数据
plt.title("商户监控-性别异常(性别为1)箱线图")
sns.boxplot(sex_table_da["取值"])
plt.xlabel("商户性别异常占比")
plt.show()
plt.savefig(input_data + r"\bad_sex_box.png")
#使用箱线图看不出异常点
#根据业务逻辑，男性画以下图（根据商户的男性占比升序）
print(sex_table_da["取值"].value_counts().sort_index())
sort_values_sex = sex_table_da["取值"].sort_values().to_list()
pd.Series(sort_values_sex).plot()
plt.title("异常性别比例排序")
plt.savefig(input_data + r"\sex_overdue_line.png")

#预警值：0.9
#风险值: 1.0

#进单时间异常点
raw_data["进单时间"] = raw_data["created_at_str"].map(lambda x: int(x.split(r" ")[1].split(r":")[0]))
pd.merge()

def get_time_table(df):
    print(df)
    flag = "未命中"
    df["early_warning_time"] = df['进单时间'].map(lambda x:x==23)
    df["risk_value_time"] = df["进单时间"].map(lambda x:(x>=0 and x<6))
    
    early_apply_no = df["apply_no"][df.early_warning_time].tolist()
    risk_apply_no = df["apply_no"][df.risk_value_time].tolist()

    if df["early_warning_time"].sum() > 1:
        flag = "预警值"
    if df["risk_value_time"].sum() > 1:
        flag = '风险值'
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["进单时间"], "取值":[-1],\
                "命中":[flag], "问题订单":[set(early_apply_no + risk_apply_no)]})
    return return_data

bad_time_list = []
for i in range(len(data_list)):
    raw_data = data_list[i]
    raw_data["进单时间"] = raw_data["created_at_str"].map(lambda x: int(x.split(r" ")[1].split(r":")[0]))
    bad_time_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "进单时间"]].apply(get_time_table)
    bad_time_list.append(bad_time_table)
    
bad_time_table = pd.concat(bad_time_list)

bad_time_table_da = bad_time_table[bad_time_table["命中"] != "未命中"]

#年龄，性别
def get_age_table(df):
    flag = '未命中'
    df["is_bad_age"] = df['age'].map(lambda x:((x>60) | (x<24)))
    bad_age_rate = df["is_bad_age"].sum()/len(df)
    df["is_bad_gender"] = df['gender'].map(lambda x:x==1)
    bad_gender_rate = df["is_bad_gender"].sum()/len(df)
    if len(df)>3:
        flag = "风险值"
        
    risk_apply_no = df["apply_no"][df.is_bad_age]
    
    return_data = pd.DataFrame({"商户门店ID":[df.iloc[0, 0]], "特征":["年龄性别"], "年龄取值":[bad_age_rate],\
                "性别取值":[bad_gender_rate] ,"命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

age_list = []
for i in range(len(data_list)):
    raw_data = data_list[i]
    age_table = raw_data.groupby("merchant_id")[["merchant_id", "apply_no", "age", "gender"]].apply(get_age_table)
    age_list.append(age_table)

age_table = pd.concat(age_list)








