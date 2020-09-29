# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 13:40:36 2020

@author: host
售后报表是指翼支付20200907的售后报表
翼支付所有数据,是指数仓里面的全部订单


年龄分段太多，导致程序运行慢
解决办法：
"""

import pandas as pd
import pymongo
from matplotlib import pyplot as plt
from sklearn.tree import DecisionTreeRegressor
#from sklearn.metrics import roc_auc_score
from sklearn import tree
import numpy as np
import seaborn as sns
from sklearn.model_selection import GridSearchCV


plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
userName = r'niewenjun'
passWord = r'niewenjun!@##@!'
input_data = r"C:\Users\host\Desktop\工作\商户监控\data\monitoring"

min_time = '2020-01-01 00:00:00'
max_time = '2020-05-31 23:59:59'
overdue_rate_read = True
combine = True
anlss_method = False

#翼支付
yzf_data = pd.read_csv(input_data + r"\售后报表.csv", encoding='GBK',\
                       usecols=["是否为测试单", "逾期天数", "订单号"])
yzf_data = yzf_data[yzf_data["是否为测试单"] == 0]

yzf_data["label"] = (yzf_data['逾期天数']>30).astype("int")


#调数仓,数据库处理
client = pymongo.MongoClient([CONN_ADDR])
client.dw_dws.authenticate(userName, passWord)

db = client.dw_dws

apply_orders_collections = db['apply_orders']

searchRes = apply_orders_collections.find({"apply.created_at_str":{"$gte":min_time,\
        "$lte":max_time}}, {'order_no':1, "apply_no":1,"user.mobile_md5":1, "user.base.age":1,\
         "user.base.idcard.no_md5":1,"user.base.gender":1,"_id":0, "apply.seller.id":1,\
         "apply.created_at_str":1, "user.base.address":1, "user.base.nation":1,\
         "user.company.salary":1, "user.base.marriage":1, "user.company.industry":1,\
         "user.edu.education":1})

raw_data = pd.DataFrame(list(searchRes))

raw_data["mobile_md5"] = raw_data['user'].map(lambda x:x['mobile_md5'])
raw_data['age'] = raw_data["user"].map(lambda x:x['base']['age'])
raw_data['no_md5'] = raw_data['user'].map(lambda x:x["base"]["idcard"]['no_md5'])
raw_data['gender'] = raw_data['user'].map(lambda x:x["base"]["gender"])
raw_data['merchant_id'] = raw_data["apply"].map(lambda x:x["seller"]["id"])
raw_data['created_at_str'] = raw_data['apply'].map(lambda x:x['created_at_str'])
raw_data['address'] = raw_data['user'].map(lambda x:x['base']['address'])
raw_data['nation'] = raw_data['user'].map(lambda x:x['base']['nation'])
raw_data['salary'] = raw_data['user'].map(lambda x:x['company']['salary'])
raw_data['marriage'] = raw_data['user'].map(lambda x:x['base']['marriage'])
raw_data['industry'] = raw_data['user'].map(lambda x:x['company']['industry'])
raw_data['education'] = raw_data['user'].map(lambda x:x['edu']['education'])

raw_data = raw_data.drop_duplicates(subset=["mobile_md5", "no_md5"])

#接下来按照商户逾期率进行计算,
#预处理
raw_data['created_at_date'] = raw_data['created_at_str'].map(lambda x:x.split(" ")[0])

#merge 申请表 售后表 raw_data 售后报表
yzf_data['订单号'] = yzf_data['订单号'].map(lambda x:x[1:])
test_data = pd.merge(raw_data[["apply_no", "order_no", "merchant_id", "created_at_date",\
                    "gender", "age", "address", "nation", "salary", 'marriage', 'industry',\
                    "education", "created_at_str"]],\
                                 yzf_data[["订单号", "label"]],\
                             how='left', left_on='order_no', right_on='订单号')
test_data.drop("订单号", axis=1, inplace=True)

#排序
test_data.sort_values(by=["merchant_id", "created_at_date"], inplace=True)

#overdue_rate
#['apply_no', 'order_no', 'merchant_id', 'created_at_date', 'gender', 'age', 'label']
#需要按照merchant_id, created_at_date的标签 
def get_overdue_rate(df):
    label_count = len(df['label'])
    overdue_counts = df['label'].sum()
    overdue_rate = overdue_counts/label_count
    return_dataframe = pd.DataFrame({"merchant_id":[df.iloc[0,0]], "created_at_date":\
                                     [df.iloc[0, 1]], "商户每天坏客户比例":[overdue_rate]})
    return return_dataframe
#groupby_overdue_data = test_data[test_data.label.notnull()]
groupby_overdue_data = test_data
merchant_overdue_rate = groupby_overdue_data.groupby(["merchant_id", "created_at_date"])\
[["label"]].mean()

#去掉label为空的，没有进单数据
merchant_overdue_rate = merchant_overdue_rate.dropna()
merchant_overdue_rate.reset_index(inplace=True)

if overdue_rate_read:
    merchant_overdue_rate.to_csv(input_data + "\商户每天坏客户比例.csv", index=False)
else:
    merchant_overdue_rate = pd.read_csv(input_data + "\商户每天坏客户比例.csv")

def sample_balance(train_data, num):
    train_data = train_data.sample(frac=1)
    
    bad_train_data = train_data[train_data["label"]>0.0]
    good_train_data = train_data[train_data["label"]==0.0]
    
    bad_sample = len(bad_train_data)
    good_sample = int(bad_sample*num)
    
    good_train_data_index = good_train_data.index
    good_train_data_index = np.random.choice(good_train_data_index, good_sample)
    good_train_data = good_train_data.loc[good_train_data_index]   

    train_data = pd.concat([bad_train_data, good_train_data], ignore_index=True)
    return train_data

#merchant_overdue_rate = sample_balance(merchant_overdue_rate, 0.5)
#merchant_overdue_rate = merchant_overdue_rate[merchant_overdue_rate.label>0]
#merchant_id created_at_date drop data
#sample_merchant_overdue_rate = sample_balance(merchant_overdue_rate, 1)

#exists_id_date = sample_merchant_overdue_rate[['merchant_id', 'created_at_date']]

#test_data sample 
#test_data = pd.merge(test_data, exists_id_date, on=['merchant_id', 'created_at_date'])

#进单数据
apply_count = test_data.groupby(["merchant_id", "created_at_date"])[['created_at_date']].count()
apply_count.columns = ['day_apply']
apply_count.reset_index(inplace=True)
apply_use_data = pd.merge(merchant_overdue_rate, apply_count,\
            on=['merchant_id', 'created_at_date'], how='inner')

#age 按照flag跑还是直接就看预警值与风险值################
def get_bad_age(df):
    print(len(df))
    flag = '未命中'
    df["is_bad_age"] = df['age'].map(lambda x:((x>60) | (x<24)))
    rate = df["is_bad_age"].sum()/len(df)
    if len(df)>3:
        if rate>=0.35:
            flag = "预警值"
        if rate>=0.8:
            flag = "风险值"
    return_data = pd.DataFrame({"merchant_id":df.iloc[0, 0], "created_at_date":\
                                [df.iloc[0, 1]], "异常年龄占比":[rate], "命中":[flag]})
    return return_data

def Dataframe_Age_Transform(df):
    value = df['age_b']
    value = str(value)
    df['age_b' + "_" + value] = df['rate_age_day']
    return df


def age_map(value):
    if value<23:
        return 0
    elif (value>=23) & (value<25):
        return 1
    elif (value>=25) & (value<30):
        return 2
    elif (value>=30) & (value<40):
        return 3
    elif (value>=40) & (value<50):
        return 4
    elif (value>=50) & (value<57):
        return 5
    else:
        return 6
    
if anlss_method:
    merchant_age_rate = test_data.groupby(["merchant_id", "created_at_date"])[["merchant_id", "created_at_date", "age"]].\
    apply(get_bad_age)
    merchant_age_rate.rename(columns={"命中":"年龄命中"}, inplace=True)
    merchant_age_rate.to_csv(input_data + "\old_商户年龄情况.csv", index=False)
else:
    test_data['age_bins'] = test_data['age'].map(age_map)
    merge_data = []
#    test_data = pd.merge(exists_id_date, test_data, on=['merchant_id', 'created_at_date'], how='inner')
    for i in range(7):
        age_test_data = test_data[test_data['age_bins'] == i]
        
        age_temp_bin = age_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
        age_temp_bin = age_temp_bin.reset_index()
        age_temp_bin.rename(columns={"apply_no":"age_b_" + str(i)}, inplace=True)
        
        merge_data.append(age_temp_bin.copy())
    
    use_age = merge_data[0]
    for i in range(6):
        use_age = pd.merge(use_age, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')
    
    use_age.fillna(0, inplace=True)

Age_Model_data = pd.merge(merchant_overdue_rate, use_age, how = 'inner', on=['merchant_id', 'created_at_date'])
Age_Model_data = pd.merge(Age_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(7):
    Age_Model_data['age_b_' + str(i)] = Age_Model_data['age_b_' + str(i)]/Age_Model_data['day_apply']

Age_Model_data = Age_Model_data.drop(["merchant_id","created_at_date"], axis=1)


#age_train = sample_balance(age_train, 1)    
#商户的坏客户比例 0.028726784679736748
model = DecisionTreeRegressor(max_depth=3)

"""
params = {'min_samples_split':[0.1, 0.2, 0.4]}
grid_model = GridSearchCV(model, params, cv=3)
grid_model.fit(Age_Model_data.drop("label", axis=1), Age_Model_data['label'])
model = grid_model.best_estimator_
"""

model.fit(Age_Model_data.drop("label", axis=1), Age_Model_data['label'])
train_predict = model.predict(Age_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/age_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, feature_names=Age_Model_data.drop("label", axis=1).columns ,\
                             out_file=f, special_characters=True, filled=True, rounded=True)

#性别
def Dataframe_Gender_Transform(df):
    value = df['gender']
    value = str(value)
    df['gender_b' + "_" + value] = df['rate_gender_day']
    return df
    

def get_bad_sex(df):
    print(len(df))
    flag ='未命中'
    df["is_bad_gender"] = df['gender'].map(lambda x:x==1)
    rate = df["is_bad_gender"].sum()/len(df)
    if len(df)>6:
        if rate>0.88:
            flag = "预警值"
        if rate>=0.9:
            flag = "风险值"

    return_data = pd.DataFrame({"merchant_id":[df.iloc[0, 0]], "created_at_date":\
                                [df.iloc[0, 1]], "异常性别占比":[rate], "命中":[flag]})
    return return_data

if anlss_method:
    merchant_sex_rate = raw_data.groupby(["merchant_id", "created_at_date"])[["merchant_id", "created_at_date", "gender"]].\
    apply(get_bad_sex)
    merchant_sex_rate.rename(columns={"命中":"性别命中", "商户门店ID":"merchant_id"}, inplace=True)
    merchant_sex_rate.to_csv(input_data + r"\商户性别情况.csv", index=False)


    merchant_age_rate.index = range(len(merchant_age_rate))
    merchant_overdue_rate.index = range(len(merchant_overdue_rate))
    merchant_sex_rate.index = range(len(merchant_sex_rate))
    
    analysis_data = pd.merge(merchant_age_rate, merchant_sex_rate, on=["merchant_id", "created_at_date"])
    analysis_data = pd.merge(analysis_data, merchant_overdue_rate, on=["merchant_id", "created_at_date"])
else:
    test_data['gender_bins'] = test_data['gender']
    merge_data = []
#    test_data = pd.merge(exists_id_date, test_data, on=['merchant_id', 'created_at_date'], how='inner')
    for i in range(2):
        gender_test_data = test_data[test_data['gender_bins'] == i]
        
        gender_temp_bin = gender_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
        gender_temp_bin = gender_temp_bin.reset_index()
        gender_temp_bin.rename(columns={"apply_no":"gender_b_" + str(i)}, inplace=True)
        
        merge_data.append(gender_temp_bin.copy())
    
    use_gender = merge_data[0]
    for i in range(1):
        use_gender = pd.merge(use_gender, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')
    
    use_gender.fillna(0, inplace=True)

Gender_Model_data = pd.merge(merchant_overdue_rate, use_gender, how = 'inner', on=['merchant_id', 'created_at_date'])
Gender_Model_data = pd.merge(Gender_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(2):
    Gender_Model_data['gender_b_' + str(i)] = Gender_Model_data['gender_b_' + str(i)]/Gender_Model_data['day_apply']

Gender_Model_data = Gender_Model_data.drop(["merchant_id","created_at_date"], axis=1)

#
model = DecisionTreeRegressor(max_depth=4)

"""
params = {'min_samples_leaf':[1, 3, 7, 10]}
grid_model = GridSearchCV(model, params, cv=3)
grid_model.fit(Gender_Model_data.drop("label", axis=1), Gender_Model_data['label'])
model = grid_model.best_estimator_
"""

model.fit(Gender_Model_data.drop("label", axis=1), Gender_Model_data['label'])
train_predict = model.predict(Gender_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/gender_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, feature_names=Gender_Model_data.drop("label", axis=1).columns,\
                             out_file=f, special_characters=True, filled=True, rounded=True)

#是否身份证户籍地在村
test_data['village_bins'] = test_data['address'].map(lambda x:('村' in x) |\
         ('组' in x) | ('队' in x))
test_data['village_bins'] = test_data['village_bins'].map(int)
merge_data = []

for i in range(2):
    village_test_data = test_data[test_data['village_bins'] == i]
    
    village_temp_bin = village_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    village_temp_bin = village_temp_bin.reset_index()
    village_temp_bin.rename(columns={"apply_no":"village_b_" + str(i)}, inplace=True)

    merge_data.append(village_temp_bin.copy())
    
use_village = merge_data[0]
for i in range(1):
    use_village = pd.merge(use_village, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_village.fillna(0, inplace=True)

Village_Model_data = pd.merge(merchant_overdue_rate, use_village, how = 'inner', on=['merchant_id', 'created_at_date'])
Village_Model_data = pd.merge(Village_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(2):
    Village_Model_data['village_b_' + str(i)] = Village_Model_data['village_b_' + str(i)]/Village_Model_data['day_apply']

Village_Model_data = Village_Model_data.drop(["merchant_id","created_at_date"], axis=1)

#
model = DecisionTreeRegressor(max_depth=4)
model.fit(Village_Model_data.drop("label", axis=1), Village_Model_data['label'])
train_predict = model.predict(Village_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/village_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f,\
                             feature_names=Village_Model_data.drop("label", axis=1).columns,\
                             special_characters=True, filled=True, rounded=True)

#nation 民族
test_data.fillna("汉", inplace=True)
test_data['nation_bins'] = test_data['nation'].map(lambda x:'汉' not in x)
test_data['nation_bins'] = test_data['nation_bins'].map(int)
merge_data = []

for i in range(2):
    nation_test_data = test_data[test_data['nation_bins'] == i]
    
    nation_temp_bin = nation_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    nation_temp_bin = nation_temp_bin.reset_index()
    nation_temp_bin.rename(columns={"apply_no":"nation_b_" + str(i)}, inplace=True)

    merge_data.append(nation_temp_bin.copy())
    
use_nation = merge_data[0]
for i in range(1):
    use_nation = pd.merge(use_nation, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_nation.fillna(0, inplace=True)

Nation_Model_data = pd.merge(merchant_overdue_rate, use_nation, how = 'inner', on=['merchant_id', 'created_at_date'])
Nation_Model_data = pd.merge(Nation_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(2):
    Nation_Model_data['nation_b_' + str(i)] = Nation_Model_data['nation_b_' + str(i)]/Nation_Model_data['day_apply']

Nation_Model_data = Nation_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=5)
model.fit(Nation_Model_data.drop("label", axis=1), Nation_Model_data['label'])
train_predict = model.predict(Nation_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/nation_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f, feature_names=Nation_Model_data.drop("label", axis=1).columns,
                             special_characters=True, filled=True, rounded=True)

#salary
test_data['salary_bins'] = test_data['salary']
merge_data = []

for i in range(5):
    salary_test_data = test_data[test_data['salary_bins'] == i]
    
    salary_temp_bin = salary_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    salary_temp_bin = salary_temp_bin.reset_index()
    salary_temp_bin.rename(columns={"apply_no":"salary_b_" + str(i)}, inplace=True)

    merge_data.append(salary_temp_bin.copy())
    
use_salary = merge_data[0]
for i in range(4):
    use_salary = pd.merge(use_salary, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_salary.fillna(0, inplace=True)

Salary_Model_data = pd.merge(merchant_overdue_rate, use_salary, how = 'inner', on=['merchant_id', 'created_at_date'])
Salary_Model_data = pd.merge(Salary_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(5):
    Salary_Model_data['salary_b_' + str(i)] = Salary_Model_data['salary_b_' + str(i)]/Salary_Model_data['day_apply']

Salary_Model_data = Salary_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=4)
model.fit(Salary_Model_data.drop("label", axis=1), Salary_Model_data['label'])
train_predict = model.predict(Salary_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/salary_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f,\
        feature_names=Salary_Model_data.drop("label", axis=1).columns, 
        special_characters=True, filled=True, rounded=True)

#marriage
test_data['marriage_bins'] = test_data['marriage'].map({1:0, 2:1, 3:1})
test_data['marriage_bins'].fillna(1, inplace=True)
test_data['marriage_bins'] = test_data['marriage_bins'].astype('int')
merge_data = []

for i in range(2):
    marriage_test_data = test_data[test_data['marriage_bins'] == i]
    
    marriage_temp_bin = marriage_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    marriage_temp_bin = marriage_temp_bin.reset_index()
    marriage_temp_bin.rename(columns={"apply_no":"marriage_b_" + str(i)}, inplace=True)

    merge_data.append(marriage_temp_bin.copy())
    
use_marriage = merge_data[0]
for i in range(1):
    use_marriage = pd.merge(use_marriage, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_marriage.fillna(0, inplace=True)

Marriage_Model_data = pd.merge(merchant_overdue_rate, use_marriage, how = 'inner', on=['merchant_id', 'created_at_date'])
Marriage_Model_data = pd.merge(Marriage_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(2):
    Marriage_Model_data['marriage_b_' + str(i)] = Marriage_Model_data['marriage_b_' + str(i)]/Marriage_Model_data['day_apply']

Marriage_Model_data = Marriage_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=4)
model.fit(Marriage_Model_data.drop("label", axis=1), Marriage_Model_data['label'])
train_predict = model.predict(Marriage_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/marriage_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f,\
                             feature_names=Marriage_Model_data.drop("label", axis=1).columns,\
                             special_characters=True, filled=True, rounded=True)

#industry
test_data['industry_bins'] = test_data['industry'].astype('int')
merge_data = []

for i in range(21):
    industry_test_data = test_data[test_data['industry_bins'] == i]
    
    industry_temp_bin = industry_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    industry_temp_bin = industry_temp_bin.reset_index()
    industry_temp_bin.rename(columns={"apply_no":"industry_b_" + str(i)}, inplace=True)

    merge_data.append(industry_temp_bin.copy())
    
use_industry = merge_data[0]
for i in range(20):
    use_industry = pd.merge(use_industry, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_industry.fillna(0, inplace=True)

Industry_Model_data = pd.merge(merchant_overdue_rate, use_industry, how = 'inner', on=['merchant_id', 'created_at_date'])
Industry_Model_data = pd.merge(Industry_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(21):
    Industry_Model_data['industry_b_' + str(i)] = Industry_Model_data['industry_b_' + str(i)]/Industry_Model_data['day_apply']

Industry_Model_data = Industry_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=4)
model.fit(Industry_Model_data.drop("label", axis=1), Industry_Model_data['label'])
train_predict = model.predict(Industry_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/industry_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f, feature_names=Industry_Model_data.drop("label", axis=1).columns,\
                             special_characters=True, filled=True, rounded=True)

#education
test_data['education_bins'] = test_data['education'].astype('int')
merge_data = []

for i in range(7):
    education_test_data = test_data[test_data['education_bins'] == i]
    
    education_temp_bin = education_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    education_temp_bin = education_temp_bin.reset_index()
    education_temp_bin.rename(columns={"apply_no":"education_b_" + str(i)}, inplace=True)

    merge_data.append(education_temp_bin.copy())
    
use_education = merge_data[0]
for i in range(6):
    use_education = pd.merge(use_education, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_education.fillna(0, inplace=True)

Education_Model_data = pd.merge(merchant_overdue_rate, use_education, how = 'inner', on=['merchant_id', 'created_at_date'])
Education_Model_data = pd.merge(Education_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(7):
    Education_Model_data['education_b_' + str(i)] = Education_Model_data['education_b_' + str(i)]/Education_Model_data['day_apply']

Education_Model_data = Education_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=4)
model.fit(Education_Model_data.drop("label", axis=1), Education_Model_data['label'])
train_predict = model.predict(Education_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/education_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f,\
                             feature_names=Education_Model_data.drop("label", axis=1).columns,\
                             special_characters=True, filled=True, rounded=True)

#异常时间节点
test_data['hour'] = test_data['created_at_str'].map(lambda x:x.split(" ")[1].split(":")[0])
test_data['hour'] = test_data['hour'].map(int)
test_data['hour'] = test_data['hour'].map(lambda x:x)
test_data['hour_bins'] = test_data['hour'].astype('int')

merge_data = []

for i in range(21):
    hour_test_data = test_data[test_data['hour_bins'] == i]
    
    hour_temp_bin = hour_test_data.groupby(["merchant_id", "created_at_date"])[['apply_no']].count()
    hour_temp_bin = hour_temp_bin.reset_index()
    hour_temp_bin.rename(columns={"apply_no":"hour_b_" + str(i)}, inplace=True)

    merge_data.append(hour_temp_bin.copy())
    
use_hour = merge_data[0]
for i in range(20):
    use_hour = pd.merge(use_hour, merge_data[i+1], on=['merchant_id', 'created_at_date'], how='outer')

use_hour.fillna(0, inplace=True)

Hour_Model_data = pd.merge(merchant_overdue_rate, use_hour, how = 'inner', on=['merchant_id', 'created_at_date'])
Hour_Model_data = pd.merge(Hour_Model_data, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

for i in range(21):
    Hour_Model_data['hour_b_' + str(i)] = Hour_Model_data['hour_b_' + str(i)]/Hour_Model_data['day_apply']

Hour_Model_data = Hour_Model_data.drop(["merchant_id","created_at_date"], axis=1)

model = DecisionTreeRegressor(max_depth=4)
model.fit(Hour_Model_data.drop("label", axis=1), Hour_Model_data['label'])
train_predict = model.predict(Hour_Model_data.drop("label", axis=1))

sns.distplot(train_predict)

with open(r"./picture/hour_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f, feature_names=Hour_Model_data.drop("label", axis=1).columns,\
                             special_characters=True, filled=True, rounded=True)



########################################特征 merge################################
#merge age gender
merge_age_gender = pd.merge(use_age, use_gender, on=['merchant_id', 'created_at_date'], how='inner')
merge_age_gender = pd.merge(merchant_overdue_rate, merge_age_gender, how='inner', on=['merchant_id', 'created_at_date'])
merge_age_gender = pd.merge(merge_age_gender, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

Merge_Age_Gender_Model_data = merge_age_gender

Merge_Age_Gender_Model_data = Merge_Age_Gender_Model_data.drop(['merchant_id', 'created_at_date'], axis=1)
AG_columns = Merge_Age_Gender_Model_data.columns.to_list()
AG_columns.remove("label")
AG_columns.remove("day_apply")
for column in AG_columns:
    Merge_Age_Gender_Model_data[column] = Merge_Age_Gender_Model_data[column]/Merge_Age_Gender_Model_data['day_apply']

A_G = Merge_Age_Gender_Model_data.drop("label", axis=1)
model = DecisionTreeRegressor(max_depth=4)
model.fit(A_G, Merge_Age_Gender_Model_data['label'])
train_predict = model.predict(A_G)

sns.distplot(train_predict)

with open(r"./picture/age_gender_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f, feature_names=A_G.columns , special_characters=True, filled=True, rounded=True)

#merge age gender village
village_merge_age_gender = pd.merge(use_age, use_gender, on=['merchant_id', 'created_at_date'], how='inner')
village_merge_age_gender = pd.merge(village_merge_age_gender, use_village, on=['merchant_id', 'created_at_date'], how='inner')
village_merge_age_gender = pd.merge(merchant_overdue_rate, village_merge_age_gender, how='inner', on=['merchant_id', 'created_at_date'])
#village_merge_age_gender = pd.merge(village_merge_age_gender, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

Village_Merge_Age_Gender_Model_data = village_merge_age_gender

Village_Merge_Age_Gender_Model_data = Village_Merge_Age_Gender_Model_data.\
drop(['merchant_id', 'created_at_date'], axis=1)

A_G_V = Village_Merge_Age_Gender_Model_data.drop("label", axis=1)
model = DecisionTreeRegressor(max_depth=4)
model.fit(A_G_V, Village_Merge_Age_Gender_Model_data['label'])
train_predict = model.predict(A_G_V)

sns.distplot(train_predict)

with open(r"./picture/village_age_gender_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, out_file=f, feature_names=A_G_V.columns , special_characters=True, filled=True, rounded=True)


#####################################概括#############################

#申请单量
warning_apply_count = Age_Model_data[(Age_Model_data.day_apply<=30.5)&(Age_Model_data.day_apply>13.5)]
risk_apply_count = Age_Model_data[(Age_Model_data.day_apply>30.5)]

#性别
warning_gender = Gender_Model_data[(Gender_Model_data.day_apply>5.5) & (Gender_Model_data.gender_b_1>0.73)&\
                (Gender_Model_data.day_apply<=10.5)]
risk_gender = Gender_Model_data[(Gender_Model_data.day_apply<=30.5) & \
                (Gender_Model_data.gender_b_1>0.73)&\
                (Gender_Model_data.day_apply>10.5)]

#nation
risk_nation = Nation_Model_data[(Nation_Model_data.day_apply<=30.5)&\
                                (Nation_Model_data.nation_b_1>0.577)&\
                                (Nation_Model_data.day_apply>24.5)]

warning_nation = Nation_Model_data[(Nation_Model_data.nation_b_1>0.042) &\
                                   (Nation_Model_data.day_apply<=30.5) &\
                                   (Nation_Model_data.nation_b_1<=0.577)&\
                                   (Nation_Model_data.day_apply>4.5)]

#salary
risk_salary = Salary_Model_data[(Salary_Model_data.salary_b_3>0.065)&\
                  (Salary_Model_data.day_apply>10)&\
                  (Salary_Model_data.day_apply<=30.5)&\
                  (Salary_Model_data.salary_b_4>0.1)&\
                  (Salary_Model_data.salary_b_4<=0.2)]

warning_salary = Salary_Model_data[(Salary_Model_data.salary_b_3>0.065)&\
                  (Salary_Model_data.day_apply>10)&\
                  (Salary_Model_data.day_apply<=30.5)&\
                  (Salary_Model_data.salary_b_4<=0.1)&\
                  (Salary_Model_data.salary_b_4>0)]

ab = Salary_Model_data[(Salary_Model_data.salary_b_3>0.065)&\
                  (Salary_Model_data.day_apply>10)&\
                  (Salary_Model_data.day_apply<=30.5)]

sns.scatterplot(x='salary_b_4', y='label', data=ab)

#marriage
"""
risk_marriage = Marriage_Model_data[(Marriage_Model_data.day_apply>7.5) &\
                                       (Marriage_Model_data.day_apply<=30.5) &\
                                       (Marriage_Model_data.marriage_b_0>0.043) &\
                                       (Marriage_Model_data.marriage_b_0<=0.049)]

risk_marriage = Marriage_Model_data[() &
                                    () &
                                    ()]

#education
risk_education = Education_Model_data[(Education_Model_data.day_apply<=30.5)&
                     (Education_Model_data.day_apply>24.5)&
                     (Education_Model_data.education_b_3>0.205)]

warning_education = Education_Model_data[(Education_Model_data.day_apply<=30.5)&\
                                         (Education_Model_data.education_b_3>0.205)&\
                                         (Education_Model_data.education_b_2>0.325)]
"""

#day_apply
#merge age gender
merge_age_gender = pd.merge(use_age, use_gender, on=['merchant_id', 'created_at_date'], how='inner')
merge_age_gender = pd.merge(merchant_overdue_rate, merge_age_gender, how='inner', on=['merchant_id', 'created_at_date'])
merge_age_gender = pd.merge(merge_age_gender, apply_count, how='inner', on=['merchant_id', 'created_at_date'])

Merge_Age_Gender_Model_data = merge_age_gender

Merge_Age_Gender_Model_data = Merge_Age_Gender_Model_data.drop(['merchant_id', 'created_at_date'], axis=1)
AG_columns = Merge_Age_Gender_Model_data.columns.to_list()
AG_columns.remove("label")
AG_columns.remove("day_apply")
for column in AG_columns:
    Merge_Age_Gender_Model_data[column] = Merge_Age_Gender_Model_data[column]/Merge_Age_Gender_Model_data['day_apply']

A_G = Merge_Age_Gender_Model_data.drop("label", axis=1)

risk_a_g = Merge_Age_Gender_Model_data[(Merge_Age_Gender_Model_data.day_apply>5.5) &\
    (Merge_Age_Gender_Model_data.day_apply<=30.5) &\
    (Merge_Age_Gender_Model_data.age_b_0>0.207)&\
    (Merge_Age_Gender_Model_data.gender_b_1>0.73)]

warning_a_g = Merge_Age_Gender_Model_data[(Merge_Age_Gender_Model_data.day_apply>5.5) &\
    (Merge_Age_Gender_Model_data.day_apply<=30.5) &\
    (Merge_Age_Gender_Model_data.age_b_0<=0.207)&\
    (Merge_Age_Gender_Model_data.gender_b_1>0.73)]
















#测试画图 商户层面
#年龄命中
analysis_data.groupby("年龄命中")["商户每天坏客户比例"].mean().plot(kind='bar', rot=360)
plt.title("商户年龄命中在2020年数据上的表现")
plt.ylabel("平均商户坏客户比例")
plt.savefig()

#性别命中
analysis_data.groupby("性别命中")["商户每天坏客户比例"].mean().plot(kind='bar', rot=360)






