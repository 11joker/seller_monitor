# -*- coding: utf-8 -*-
"""
Created on Sat Aug  8 09:18:20 2020

@author: host

这个需要所有的和包审核数据
"""

import pandas as pd
import datetime

input_data = r"C:\Users\host\Desktop\工作\商户监控\data\monitoring"
all_province_hb_data = pd.read_csv(input_data + r"\和包审核数据_20200928092019.csv", encoding="GBK",\
                      usecols=['超人订单号', '门店id','门店名称' , '年龄', '身份证', '申请授信时间',\
                               '审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消',\
                               '营业厅省'])

now_time = datetime.datetime.now()
yesterday = now_time + datetime.timedelta(days=-1)
now_time = now_time.strftime('%Y-%m-%d')
yesterday = yesterday.strftime('%Y-%m-%d')

provinces_list = ['四川省\t', '青海省\t']
write = pd.ExcelWriter(r"C:\Users\host\Desktop\工作\商户监控\qyj_merchant_monitoring.xlsx")
for province in provinces_list:
    #fetch 审核 data
    hb_data = all_province_hb_data[all_province_hb_data['营业厅省'] == province]
    order_no_check = hb_data[['超人订单号', '审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消']]
    
    """
    hb_data['merge_no'] = hb_data['和包订单号'].map(lambda x:x[:-1])
    hb_service_data['merge_no'] = hb_service_data['渠道方订单号'].map(lambda x:x[1:])
    
    merge_data = pd.merge(hb_data[["merge_no", "申请日期"]], hb_service_data[["merge_no", "办理完成时间"]], on='merge_no')
    merge_data['申请日期'] = merge_data['申请日期'].map(lambda x:x[:4] + "-" + x[4:6] + "-" + x[6:8])
    merge_data['办理完成时间'] = merge_data['办理完成时间'].map(lambda x:x.split(" ")[0])
    """
    #hb_data
    id_name = hb_data[["门店id", "门店名称"]]
    id_name = id_name[id_name['门店id'] != 0]
    id_name = id_name.drop_duplicates("门店名称")
    
    #日申请单（A1）,新商户定义，找到今天的所有商户，根据历史售后数据（办理完成时间），找到这些商户的最早办理完成时间，与今天时间进行比较
    def get_hit(value):
        flag = '未命中'
        if value>15:
            flag =  "预警值"
        if value>20:
            flag =  "风险值"
        return flag
    
    all_hb_data = hb_data
    hb_data["申请日期"] = hb_data['超人订单号'].map(lambda x:x[2:6] + "-" + x[6:8] + "-" + x[8:10])
    #今天要统的数据, 
    hb_data = hb_data[hb_data['申请日期'] == yesterday]
    select_hb_data = all_hb_data[all_hb_data['门店名称'].isin(list(set(hb_data['门店名称'])))]
    select_hb_data = select_hb_data[select_hb_data["审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消"] == 3]
    min_select_hb_data = select_hb_data.groupby("门店名称")[['申请日期']].min()
    min_select_hb_data = min_select_hb_data.reset_index()
    min_select_hb_data.columns = ["门店名称", "申请日期"]
    
    def is_behave_time(value):
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
    
    min_select_hb_data['商户是否进入表现期'] = min_select_hb_data['申请日期'].map(is_behave_time)
    merchant_list = list(set(min_select_hb_data[min_select_hb_data["商户是否进入表现期"]==0]["门店名称"]))
    new_merchant = hb_data[hb_data["门店名称"].isin(merchant_list)]
    
    apply_count = new_merchant["门店名称"].value_counts().to_frame().reset_index()
    apply_count.columns = ["门店名称", "取值"]
    apply_count['特征'] = "日申请单(A1)"
    apply_count["命中"] = apply_count["取值"].map(get_hit)
    apply_count['问题订单'] = "没有"
    
    
    #日申请单(A2)
    temp_apply_count = hb_data["门店名称"].value_counts().to_frame().reset_index()
    temp_apply_count.columns = ["门店名称", "取值"]
    temp_apply_count['特征'] = "日申请单(A1)"
    temp_apply_count["命中"] = temp_apply_count["取值"].map(get_hit)
    
    def get_average_apply(df):
        days = len(set(df["申请日期"]))
        apply_number = len(df['申请日期'])
        average_apply_number = (apply_number/days)
        return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "日均申请单量":average_apply_number})
        return return_data
        
    all_hb_data["申请日期"] = all_hb_data['超人订单号'].map(lambda x:x[2:10])
    average_apply_number = all_hb_data.groupby('门店名称')[["门店名称", "申请日期"]].apply(get_average_apply)
    average_apply_number.index = range(len(average_apply_number))
    
    #需要使用A1,得到A2
    def get_day_apply_A2(series):
        flag='未命中'
        day_average_apply = series['日均申请单量']
        today_apply = series["取值"]
        if (day_average_apply>=1) and (day_average_apply<=3) and (today_apply>=(day_average_apply*3)):
            flag = '预警值'
        if (day_average_apply>=4) and (today_apply>=(day_average_apply*2.5)):
            flag= '预警值'
        if (day_average_apply>=1) and (day_average_apply<=3) and (today_apply>=(day_average_apply*3.5)):
            flag = '风险值'
        if (day_average_apply>=4) and (today_apply>=(day_average_apply*3)):
            flag = '风险值'
        return_data = pd.Series({"门店名称":series['门店名称'], "特征":"日申请单(A2)", "取值":[today_apply, day_average_apply],\
                    "命中":flag, "问题订单":"没有"})
        return return_data
    temp_apply_data = pd.merge(temp_apply_count[['门店名称', '取值']], average_apply_number, on='门店名称', how='left')
    average_apply_number_A2 = temp_apply_data.apply(get_day_apply_A2, axis=1)
    
    #重复订单
    
    
    #申请授信时间
    def get_time_table(df):
        flag = '未命中'
        df["进单时间"] = df["申请授信时间"].map(lambda x: int(x.split(r" ")[1].split(r":")[0]))
        df["early_warning_time"] = df['进单时间'].map(lambda x:x==23)
        df["risk_value_time"] = df["进单时间"].map(lambda x:(x>=0 and x<6))
        
        early_apply_no = df["超人订单号"][df.early_warning_time].tolist()
        risk_apply_no = df["超人订单号"][df.risk_value_time].tolist()
        
        if df["early_warning_time"].sum() > 0:
            flag = '预警值'
        if df["risk_value_time"].sum() > 0:
            flag = "风险值"
        
        return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "特征":["进单时间"], "取值":[-1],\
                    "命中":[flag], "问题订单":[set(early_apply_no + risk_apply_no)]})
        return return_data
    time_point_table = hb_data.groupby("门店名称")[["门店名称", "超人订单号", "申请授信时间"]].apply(get_time_table)
    
    
    #age
    def get_age_table(df):
        flag = "未命中"
        df["is_bad_age"] = df['年龄'].map(lambda x:((x>65) | (x<25)))
        rate = df["is_bad_age"].sum()/len(df)
        if len(df)>3:
            if rate>0.5:
                flag = "预警值"
            if rate>0.9:
                flag = "风险值"
        
        risk_apply_no = df["超人订单号"][df.is_bad_age]
        return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "特征":["年龄"],  "取值":[rate],\
                    "命中":[flag], "问题订单":[set(risk_apply_no)]})
    
        return return_data
    age_table = hb_data.groupby("门店名称")[["门店名称", "超人订单号", "年龄"]].apply(get_age_table)
    
    #性别
    def get_sex_table(df):
        flag = "未命中"
        df["is_bad_gender"] = df['性别'].map(lambda x:x==1)
        rate = df["is_bad_gender"].sum()/len(df)
        if len(df)>4:
            if rate>0.95:
                flag = "预警值"
            if rate==1:
                flag = "风险值"
            
        risk_apply_no = df["超人订单号"][df.is_bad_gender]
        
        return_data = pd.DataFrame({"门店名称":[df.iloc[0, 0]], "特征":["性别"], "取值":[rate],\
                    "命中":[flag], "问题订单":[set(risk_apply_no)]})
        return return_data
    hb_data["性别"] = hb_data["身份证"].map(lambda x:(int(x[-3])%2) if x[-3]!="*" else 1)
    sex_table = hb_data.groupby("门店名称")[["门店名称", "超人订单号", "性别"]].apply(get_sex_table)
    
    
    #总concat
    jyj_merchant_table = pd.concat([apply_count, average_apply_number_A2, time_point_table, age_table, sex_table])
    
    jyj_merchant_table['风险等级'] = jyj_merchant_table["命中"].map({"未命中":1, "预警值":2, "风险值":3})
    
    jyj_merchant_table = jyj_merchant_table[["门店名称", "特征", "命中", "取值", "风险等级", "问题订单"]]
    #save
    important_jyj_merchant_table = jyj_merchant_table[jyj_merchant_table['命中'] != '未命中']
    
    important_jyj_merchant_table = pd.merge(important_jyj_merchant_table, id_name, on='门店名称', how='left')
    
    
    proplem_order_set = set()
    proplem_order = important_jyj_merchant_table[important_jyj_merchant_table['问题订单']!='没有']['问题订单']
    proplem_order = proplem_order.to_list()
    
    for set_value in proplem_order:
        proplem_order_set = (proplem_order_set | set_value)
    proplem_order_list = list(proplem_order_set)
    proplem_order_list = list(map(lambda x:x[:-1], proplem_order_list))
    
    proplem_order = pd.DataFrame()
    proplem_order['问题订单列'] = proplem_order_list
    
    #筛选订单
    order_no_check['超人订单号'] = order_no_check['超人订单号'].map(lambda x:x[:-1])
    proplem_order = pd.merge(proplem_order, order_no_check, how='left', left_on='问题订单列',\
             right_on='超人订单号')
    proplem_order = proplem_order[proplem_order["审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消"]==3]
    proplem_order.drop(["超人订单号", "审核状态：0待自动审核 1自动审核中 2待人工审核 3审核通过 4审核拒绝 5审核取消"],axis=1, inplace=True)
    
    proplem_order.to_excel(excel_writer=write, sheet_name= province.strip() + '问题订单汇总', index=False)
    important_jyj_merchant_table.to_excel(excel_writer=write, sheet_name = province.strip() + "权益金可疑商户", index = False)
    jyj_merchant_table.to_excel(excel_writer=write, sheet_name = province.strip() + "权益金原始数据", index=False)
    
write.save()
write.close()



