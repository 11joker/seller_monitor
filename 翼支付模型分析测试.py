# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:37:57 2020

@author: host
"""
import pymongo
import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeRegressor 
from sklearn import tree

input_data = r"C:\Users\host\Desktop\工作\商户监控\data\monitoring"
CONN_ADDR = r'dds-wz9b9289b707b2c41974-pub.mongodb.rds.aliyuncs.com:3717'
userName = r'niewenjun'
passWord = r'niewenjun!@##@!'

min_time = '2020-06-01 00:00:00'
max_time = '2020-07-31 23:59:59'
#0.0091
anlss_method = False

feature_list = ['order_no', 'apply_no', "user.mobile_md5", "user.base.age",\
                "user.base.idcard.no_md5", 'user.base.gender', 'apply.seller.id',\
                "apply.created_at_str", "user.base.address", "user.base.nation",\
                "user.company.salary", "user.base.marriage", "user.company.industry",\
                "user.edu.education"]

yzf_data = pd.read_csv(input_data + r"\售后报表.csv", encoding='GBK',\
                       usecols=["是否为测试单", "逾期天数", "订单号"])
yzf_data = yzf_data[yzf_data["是否为测试单"] == 0]
yzf_data["label"] = (yzf_data['逾期天数']>30).astype("int")

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
    
yzf_data_warehouse = YZF_GET_DATA_AWAREHOUSE()  
raw_data = yzf_data_warehouse.get_transform_data(feature_list=feature_list,\
                            database='dw_dws', collection='apply_orders',\
                            value_range=[min_time, max_time])
raw_data['created_at_date'] = raw_data['created_at_str'].map(lambda x:x.split(" ")[0])
raw_data['merchant_id'] = raw_data['id']

yzf_data['订单号'] = yzf_data['订单号'].map(lambda x:x[1:])
test_data = pd.merge(raw_data[["apply_no", "order_no", "merchant_id", "created_at_date",\
                    "gender", "age", "address", "nation", "salary", 'marriage', 'industry',\
                    "education"]], yzf_data[["订单号", "label"]], how='left',\
                     left_on='order_no', right_on='订单号')

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

#age
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

model = DecisionTreeRegressor(max_depth=3)
model.fit(Age_Model_data[['day_apply', 'age_b_5']], Age_Model_data[['label']])

with open(r"./picture/test_age_apply.dot", 'w') as f:
    f = tree.export_graphviz(model, feature_names=['day_apply', 'age_b_5'] ,\
                             out_file=f, special_characters=True, filled=True, rounded=True)

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

#性别
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


#





    