# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 08:42:04 2020

@author: host
权益金数据分析:在7月份上进行分析
"""

import pandas as pd
import numpy as np
import datetime
import seaborn as sns
from matplotlib import pyplot as plt
import toad

plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

input_data = r"C:\Users\host\Desktop\工作\商户监控\data\analysis"
hb_data = pd.read_csv(input_data + r"\和包审核数据_20200812091118.csv", encoding="GBK")
hb_after_data = pd.read_csv(input_data + r'\权益金售后报表_2020081311.csv', encoding='GBK')
hb_after_data = hb_after_data[(hb_after_data["办理完成时间"]>="2020-07-21 00:00:00") & (hb_after_data['办理完成时间']<="2020-07-31 23:59:59")]
hb_after_data = hb_after_data[hb_after_data['是否为测试单'] == 0]

now_time = datetime.datetime(2020, 8, 12, 9, 42, 40, 159409)
time_list = []
for i in range(15):
    shift_time = now_time + datetime.timedelta(days=-(i+1))
    shift_time = shift_time.strftime('%Y-%m-%d')
    time_list.append(shift_time)


"""
hb_data['merge_no'] = hb_data['和包订单号'].map(lambda x:x[:-1])
hb_service_data['merge_no'] = hb_service_data['渠道方订单号'].map(lambda x:x[1:])

merge_data = pd.merge(hb_data[["merge_no", "申请日期"]], hb_service_data[["merge_no", "办理完成时间"]], on='merge_no')
merge_data['申请日期'] = merge_data['申请日期'].map(lambda x:x[:4] + "-" + x[4:6] + "-" + x[6:8])
merge_data['办理完成时间'] = merge_data['办理完成时间'].map(lambda x:x.split(" ")[0])
"""
hb_data["申请日期"] = hb_data["申请授信时间"].map(lambda x:x.split(" ")[0])
hb_data_list = []
for time in time_list:
    hb_data_list.append(hb_data[hb_data['申请日期'] == time])

#------------------计算每天平均申请单量和商户数量
apply_sum = 0
merchant_sum = 0
for i in range(len(hb_data_list)):
    raw_data = hb_data_list[i]
    today_sum = len(raw_data)
    apply_sum += today_sum
    merchant_sum += len(set(raw_data['门店名称']))

print("每日平均进单数量:", apply_sum/len(hb_data_list))
print("每日平均进单商户数量:", merchant_sum/len(hb_data_list))
"""
每日平均进单数量: 6891.533333333334
每日平均进单商户数量: 2709.8
"""

#日申请单（A1）,新商户定义，找到今天的所有商户，根据历史售后数据（办理完成时间），找到这些商户的最早办理完成时间，与今天时间进行比较
def get_hit(value):
    flag = '未命中'
    if value>10:
        flag =  "预警值"
    if value>15:
        flag =  "风险值"
    return flag

all_hb_data = hb_data
hb_data["申请日期"] = hb_data['超人订单号'].map(lambda x:x[2:6] + "-" + x[6:8] + "-" + x[8:10])
#今天要统的数据, 

def is_behave_time(series):
    value = series['申请日期']
    yesterday = series['time']
    if value =="--":
        return 1
    value = datetime.datetime.strptime(value, "%Y-%m-%d")
    temp_yesterday = datetime.datetime.strptime(yesterday, "%Y-%m-%d")
    delta_days = (temp_yesterday - value).days
    if delta_days>30:
        return 1
    if temp_yesterday.month>value.month and temp_yesterday.day<=9:
        return 0 
    elif temp_yesterday.month>value.month and temp_yesterday.day>9:
        return 1
    else:
        return 0  

apply_count_merge = []
for time in time_list:
    temp_hb_data = hb_data[hb_data['申请日期'] == time]
    select_hb_data = all_hb_data[all_hb_data['门店名称'].isin(list(set(temp_hb_data['门店名称'])))]
    select_hb_data = select_hb_data[select_hb_data["审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消"] == 3]
    min_select_hb_data = select_hb_data.groupby("门店名称")[['申请日期']].min()
    min_select_hb_data = min_select_hb_data.reset_index()
    min_select_hb_data.columns = ["门店名称", "申请日期"]

    min_select_hb_data['time'] = time
    min_select_hb_data['商户是否进入表现期'] = min_select_hb_data[['申请日期', 'time']].apply(is_behave_time, axis=1)
    merchant_list = list(set(min_select_hb_data[min_select_hb_data["商户是否进入表现期"]==0]["门店名称"]))
    new_merchant = temp_hb_data[temp_hb_data["门店名称"].isin(merchant_list)]
    
    apply_count = new_merchant["门店名称"].value_counts().to_frame().reset_index()
    apply_count.columns = ["门店名称", "取值"]
    apply_count['特征'] = "日申请单(A1)"
    apply_count["命中"] = apply_count["取值"].map(get_hit)
    apply_count['问题订单'] = "没有"
    apply_count_merge.append(apply_count)
    
all_apply_count = pd.concat(apply_count_merge)
sns.boxplot(all_apply_count['取值'])

#排序
print(all_apply_count["取值"].value_counts().sort_index())
sort_values_apply = all_apply_count["取值"].sort_values().to_list()
pd.Series(sort_values_apply).plot()
plt.xlabel("排序之后第N个商户")
plt.ylabel("日申请单（A1）")

#-------------age----------
age_bad_people = hb_after_data[["年龄", "逾期天数"]]
age_bad_people["label"] = age_bad_people['逾期天数']>0
#age_bad_people['年龄'] = pd.cut(age_bad_people["年龄"], bins=6)
combiner = toad.transform.Combiner()
combiner.fit(age_bad_people[["年龄", "label"]], y='label',method='chi', n_bins=5)
combiner.rules
age_combiner = combiner.transform(age_bad_people[["年龄", "label"]])
age_combiner["年龄"] = age_combiner['年龄'].map({0:"(-,32)", 1:"[32, 44)", 2:"[44,51)", 3:"[50,56)", 4:"[54,+)"})
age_combiner.groupby("年龄")["label"].mean().plot(kind='bar', rot=360)
plt.ylabel("逾期率")
"""
[54,+)      4885
[44,51)     3999
[32, 44)    3081
[50,56)     1856
(-,32)      1386
"""
def get_age_table(df):
    flag = "未命中"
    df["is_bad_age"] = df['年龄'].map(lambda x:((x>65) | (x<32)))
    rate = df["is_bad_age"].sum()/len(df)
    if len(df)>3:
        flag = "风险值"
    
    risk_apply_no = df["超人订单号"][df.is_bad_age]
    return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "特征":["年龄"],  "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})

    return return_data

age_list = []
for i in range(len(hb_data_list)):
    hb_data = hb_data_list[i]
    hb_data['年龄'] = hb_data['年龄'].map(lambda x: int(x))
    age_table = hb_data.groupby("门店名称")[["门店名称", "超人订单号", "年龄"]].apply(get_age_table)
    age_list.append(age_table)

age_table = pd.concat(age_list)
age_table_da = age_table[age_table["命中"] == "风险值"]
#筛除商户日进单<=2的商户，4天的商户数据
plt.title("商户监控-年龄异常(大于65小于32)箱线图")
sns.boxplot(age_table_da["取值"])
plt.xlabel("商户年龄异常占比")

#排序
print(age_table_da["取值"].value_counts().sort_index())
sort_values_apply = age_table_da["取值"].sort_values().to_list()
pd.Series(sort_values_apply).plot()
plt.xlabel("排序之后第N个商户")
plt.ylabel("年龄异常比例")
plt.title("排序之后第N个商户的申请年龄比例")

#--------------性别-----------------
hb_after_data['性别'] = hb_after_data['身份证'].map(lambda x:(int(x[-3])%2) if x[-3]!="*" else 1)
age_bad_people = hb_after_data[["性别", "逾期天数"]]
age_bad_people["label"] = age_bad_people['逾期天数']>0
age_bad_people.groupby("性别")["label"].mean().plot(kind='bar', rot=360)
plt.ylabel("逾期率")

def get_sex_table(df):
    flag = "未命中"
    df["is_bad_gender"] = df['性别'].map(lambda x:x==1)
    rate = df["is_bad_gender"].sum()/len(df)
    if len(df)>4:
        flag = "风险值"
        
    risk_apply_no = df["超人订单号"][df.is_bad_gender]
    
    return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "特征":["性别"], "取值":[rate],\
                "命中":[flag], "问题订单":[set(risk_apply_no)]})
    return return_data

sex_list = []
for i in range(len(hb_data_list)):
    hb_data = hb_data_list[i]
    hb_data["性别"] = hb_data["身份证"].map(lambda x:(int(x[-3])%2) if x[-3]!="*" else 1)
    sex_table = hb_data.groupby("门店名称")[["门店名称", "超人订单号", "性别"]].apply(get_sex_table)
    sex_list.append(sex_table)

sex_table = pd.concat(sex_list)
sex_table_da = sex_table[sex_table["命中"] == "风险值"]
#筛除商户日进单<=2的商户，4天的商户数据
plt.title("商户监控-性别异常(性别为1)箱线图")
sns.boxplot(sex_table_da["取值"])
plt.xlabel("商户性别异常占比")
#使用箱线图看不出异常点
#根据业务逻辑，男性画以下图（根据商户的男性占比升序）
print(sex_table_da["取值"].value_counts().sort_index())
sort_values_sex = sex_table_da["取值"].sort_values().to_list()
pd.Series(sort_values_sex).plot()
plt.xlabel("排序之后第N个商户")
plt.ylabel("年龄异常比例")



    