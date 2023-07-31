#-*- coding: UTF-8 -*-
import sys, dateutil.parser, time, math, requests, re, json, os
import pandas as pd
import numpy as np
from datetime import datetime as dt
from collections import defaultdict
from bson import json_util
from bson.json_util import loads
import py7zr
#資料處理轉換
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
def daytime(dtime):
    if len(dtime)>=20:format="%Y-%m-%d %H:%M:%S.%f"
    elif len(dtime)==10:format="%Y-%m-%d"
    else:format="%Y-%m-%d %H:%M:%S"
    dtime = {"$date":int(json.dumps(dt.strptime(dtime,format),default=json_util.default).split()[1][:-1])}
    return dtime

def tojson(data):
    line={}
    title=list(data.values.tolist())
    if "daytime" in title:
        line.setdefault("daytime",daytime(str(data.daytime)))
        title.remove("daytime")
    for i in title:
        line.setdefault(i,data[i])
    return line

def save_json(path,filename,data):
    with open (path+filename+".json","w") as jsonf:
        jsonf_write=jsonf.write
        i = 0
        while i<len(data):
            jsonf_write(json.dumps(tojson(data.loc[i]), cls=NpEncoder)+"\n")
            i+=1
    return path+filename

# 獲取文件夾大小
def get_size_byte(path):
    size = 0
    for root, dirs, files in os.walk(path):
        size += sum([getsize(join(root, name)) for name in files])
    return size


# 2021.3.16 Lin新增 加/解壓縮 dsp重建巢狀 搓合 03-26 poi_acc 
def un_zip(file_path,target_path):
    zf.ZipFile(file_path,"r").extractall(target_path)
    return file_path
def un_7z(file_path,target_path):
    with py7zr.SevenZipFile(file_path, mode='r') as zipf:
        zipf.extractall(target_path)
    return file_path
def arc_zip(zip_path,archive_path):
    with zf.ZipFile(zip_path, "w") as zipf:
        target = os.listdir(archive_path)
        for i in target:
            zipf.write(f"{archive_path}{i}")
    return zip_path
def arc_7z(zip_path,archive_path):
    with py7zr.SevenZipFile(zip_path, 'w') as zipf:
        target = os.listdir(archive_path)
        for i in target:
            zipf.write(f"{archive_path}/{i}")
    return zip_path

def nested_bs(data,bs):
    if not isinstance(bs, str):
        bs = str(bs)
    data = pd.DataFrame(data,columns=[bs]).dropna()
    title = []
    old_title = data.T.columns.values.tolist()
    for ii in (old_title):
        ii = str(ii+1)
        C = bs[:3]+ii
        title.extend([C])
    bid_d=dict(zip(old_title,title))
    data = data.T.rename(columns=bid_d)
    data = data.to_dict("records")
    return data
    
def matching(osf_data,mtf_data):
    osf_data = osf_data.groupby(["bs","price","brkid","connect"])["aftshr"].sum().reset_index()
    mtf_data = mtf_data.groupby(["bs","brkid","connect"])["mtfshr"].sum().reset_index()
    osf_data = osf_data[osf_data["aftshr"]>0]
    osf_data=pd.merge(osf_data,mtf_data,on=["bs","connect","brkid"],how="left")
    osf_data["mtfshr"].fillna(0,inplace=True)
    osf_data["volume"]= osf_data["aftshr"]-osf_data["mtfshr"]
    osf_data = osf_data[osf_data["volume"]>0].reset_index(drop=True)
    osf_data = osf_data.groupby(["bs","price"])["volume"].sum().reset_index()
    osf_data = pd.DataFrame(osf_data,columns=["bs","price","volume"])
    b = osf_data[(osf_data["bs"]=="B")].sort_values(by=['price'], ascending=False).reset_index(drop=True)
    s = osf_data[(osf_data["bs"]=="S")].sort_values(by=['price'], ascending=True).reset_index(drop=True)
    b.rename(columns={"price":"bidp","volume":"bidv"},inplace=True)
    s.rename(columns={"price":"askp","volume":"askv"},inplace=True)
    if len(b)>len(s): long = len(b)
    else: long = len(s)
    for i in range(0,long):
        if b.loc[0,"bidp"]>=s.loc[0,"askp"]:
            if b.loc[0,"bidv"]>=s.loc[0,"askv"]:
                b.loc[0,"bidv"] = b.loc[0,"bidv"]-s.loc[0,"askv"]
                s.loc[0,"askv"] = 0
            else: 
                s.loc[0,"askv"] = s.loc[0,"askv"]-b.loc[0,"bidv"]
                b.loc[0,"bidv"]=0
            b = b[b["bidv"]>0].reset_index(drop=True)
            s = s[s["askv"]>0].reset_index(drop=True)
    #         
        else:
            break
    else:
        b = b.reset_index()
        s = s.reset_index()
        a = pd.merge(b,s, on=["index"], how='outer')
    return a 
# 2021-03-27 新增製造poi_acc表以及製作有acc的new_poi
def poi_acc(data_mtf,data_poi):
    data_mtf = data_mtf.groupby(["id","acc","contract1"]).size().reset_index().sort_values(by=["id",0],ascending=False).drop_duplicates(subset=["id","contract1"],keep = "first").rename(columns={"contract1":"contract","merchandise1":"merchandise"})      
    data_poi = data_poi.drop_duplicates(subset=["id"])
    new_poi = pd.merge(data_poi,data_mtf,on=["id","contract"],how="left").sort_values("contract").drop(columns=0)
    new_poi= new_poi.dropna()
    new_poi["acc"] = new_poi["acc"].astype(int)
    return new_Poi
def fut_newpoi (old_poi,new_poi):
    poi = old_poi.groupby(by=["daytime","contract","id"])[["tbuyv","tsellv","ybuyre","ysellre","tbuyre","tsellre"]].sum().reset_index()
    a = new_poi.groupby(["id","acc"]).size().reset_index().sort_values(["id","acc"]).groupby(["id"]).size().reset_index()
    many = a["id"][a[0]>1]
    for ii in list(many):
        check = new_poi[(new_poi["id"]==ii)].groupby(["id","acc"]).size().reset_index().sort_values(0)
        if (len(check)==2)&(check.iat[-1,2]!=check.iat[0,2]):
            check = check.iat[-1,1]
            new_poi.loc[(new_poi["id"]==ii),"acc"] = check 
        else: 
            check["contract"] = i
            problem.append(shit)
            check1 = check.sort_values("acc").iat[0,1]
            new_poi.loc[(new_poi["id"]==ii),"acc"] = check1
    final= pd.merge(poi,new_poi.drop_duplicates(subset=["id"]),on="id",how="left")
    final["acc"] = final["acc"].fillna(0).astype(int)
    return final
def opt_newpoi(old_poi,new_poi):
    poi = old_poi.groupby(by=["daytime","contract","id","strike","cp"])[["tbuyv","tsellv","ybuyre","ysellre","tbuyre","tsellre"]].sum().reset_index()
    a = new_poi.groupby(["id","acc"]).size().reset_index().sort_values(["id","acc"]).groupby(["id"]).size().reset_index()
    check = a["id"][a[0]>1]
    for ii in list(fuck):
        check = new_poi[(new_poi["id"]==ii)].groupby(["id","acc"]).size().reset_index().sort_values(0)
        if (len(check)==2)&(check.iat[-1,2]!=check.iat[0,2]):
            check = check.iat[-1,1]
            new_poi.loc[(new_poi["id"]==ii),"acc"] = check 
        elif (len(check)==2)&(check.iat[-1,2]==check.iat[-2,2]):
            check = pd.merge(new_poi[(new_poi["id"]==ii)],check,on=["id","acc"],how="left")
            check["contract"]=i
            problem.append(check)
            check1 = check.sort_values("acc").iat[0,3]
            new_poi.loc[(new_poi["id"]==ii),"acc"] = check1
        else:
            check = pd.merge(new_poi[(new_poi["id"]==ii)],check,on=["id","acc"],how="left")
            check["contract"]=i
            problem.append(check)
            check1 = check.sort_values("acc").iat[0,3]
            new_poi.loc[(new_poi["id"]==ii),"acc"] = check1
    key = new_poi.drop_duplicates(subset=["id"])[["id","acc"]]
    new_poi= pd.merge(poi,key,on=["id"],how="left")
    new_poi["acc"] = new_poi["acc"].fillna(0).astype(int)
    return new_poi
# 2021-03-21 lin新增換約表
def switching(data):
#     df.rename(columns={"期貨名稱":"contract","日期":"daytime","成交張數(量)":"volume","剩餘天數":"left_over"},inplace=True)
    data["volume"]=data["volume"].str.replace(",","").astype(int)
    data["contract"]=data["contract"].str[2:8].astype(int)
    a=data[data["left_over"]==3].index
    for i in range(len(a)):
        if i ==0:
            df1 = pd.DataFrame(data[0:a[i]-1],columns=["daytime","contract"])
            df1=df1[df1["contract"]==df.at[a[i],"contract"]]
        elif i!=len(a)-1:     
            df2 = pd.DataFrame(data[a[i-1]:a[i]-1]).reset_index(drop=False)
            df2 = df2[(df2["contract"]==df2["contract"][0])|(df2["contract"]==df2["contract"][1])].reset_index(drop=True)
            check1= df2["volume"][0]<df2["volume"][1]
            check2= df2["volume"][2]<df2["volume"][3]
            check3= df2["volume"][4]<df2["volume"][5]
            if check1==True:
                df2.drop([0,2,4],inplace=True)
            elif check2==True:
                df2.drop([1,2,4],inplace=True)
            elif check3==True:
                df2.drop([1,3,4],inplace=True)
            else:
                df2.drop([1,3,5],inplace=True)
            df2 =df2.set_index("index")
            df2.drop(columns=["volume","left_over"],inplace=True)
            df1 = pd.concat([df1,df2])
        else:
            df2 = pd.DataFrame(data[a[i]:]).reset_index(drop=False)
            df2 = df2[(df2["contract"]==df2["contract"][0])|(df2["contract"]==df2["contract"][1])].reset_index(drop=True)
            if len(df2)==2:
                check1= df2["volume"][0]<df2["volume"][1]
                if check1==True:
                    df2.drop([0],inplace=True)
                else:
                    df2.drop([1],inplace=True)
                df2 =df2.set_index("index")
                df2.drop(columns=["volume","left_over"],inplace=True)
                df1 = pd.concat([df1,df2])
            elif len(df2)==4:
                check1= df2["volume"][0]<df2["volume"][1]
                check2= df2["volume"][2]<df2["volume"][3]
                if check1==True:
                    df2.drop([0,2],inplace=True)
                elif check2==True:
                    df2.drop([1,2],inplace=True)
                else:
                    df2.drop([1,3],inplace=True)
                df2 =df2.set_index("index")
                df2.drop(columns=["volume","left_over"],inplace=True)
                df1 = pd.concat([df1,df2])
            else :
                check1= df2["volume"][0]<df2["volume"][1]
                check2= df2["volume"][2]<df2["volume"][3]
                check3= df2["volume"][4]<df2["volume"][5]
                if check1==True:
                    df2.drop([0,2,4],inplace=True)
                elif check2==True:
                    df2.drop([1,2,4],inplace=True)
                elif check3==True:
                    df2.drop([1,3,4],inplace=True)
                else:
                    df2.drop([1,3,5],inplace=True)
                df2 =df2.set_index("index")
                df2.drop(columns=["volume","left_over"],inplace=True)
                df1 = pd.concat([df1,df2])
    df1["daytime"] = pd.to_datetime(df1['daytime'].str.split('/').str[0]+"-"+df1['daytime'].str.split('/').str[1]+"-"+df1['daytime'].str.split('/').str[2],format="%Y-%m-%d")
    return df1
