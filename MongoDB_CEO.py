#-*- coding: UTF-8 -*-
import sys, dateutil.parser, time, math, requests, re, json, os, pymongo
import pandas as pd
import numpy as np
from datetime import datetime as dt
from pymongo import MongoClient
from collections import defaultdict
from bson import json_util
from bson.json_util import loads

class MD:
    def __init__(self,ip,port,user,pwd):
        self.ip = ip
        self.port = port
        self.user = user
        self.pwd = pwd
        self.client = MongoClient(self.ip,self.port)
        if self.user!=0:
            self.client.admin.authenticate(self.user,self.pwd, mechanism='SCRAM-SHA-1')
    #資料庫使用者管理
#     2021-03-27 lin 新增創建帳戶等方法
    def create_user(self,user_db,name,password,role):
        link = self.client[user_db]
        final =[]
        a = ["role","db"]
        for i in range(len(role)):
            dic = dict(zip(a,role[i]))
            final.append(dic)
        get = link.command("createUser", name, pwd = password,roles = final)
        return get
    def changepwd_user(self,user_db,name,password):
        link = self.client[user_db]
        get = link.command("updateUser", name, pwd = password)
        return get
    def upgraderole_user(self,user_db,name,role):
        link = self.client[user_db]
        final =[]
        a = ["role","db"]
        for i in range(len(role)):
            dic = dict(zip(a,role[i]))
            final.append(dic)
        get = link.command("updateUser",name,roles = final)
        return get
    def del_user(self,user_db,name):
        link = self.client[user_db]
        get = link.command("dropUser",name)
        return get
    # 標準化欄位
    def stdtitle_pd(data,daytime,stri,doub,INT):
        string = ['sc','bs','merchandise','odrcond','seqno','connect','id','date','merchandise1','cp1','merchandise2','cp2','bs1','bs2','oq','inst','cp','mark','flag','odrprt','brkid','recno','mtfprt','prscd','trend','mtf_flag','mtf_rf','buy_rf','sell_rf','userid','marketer']
        intt = ['acc','contract1','contract2','vol','devol','unvol','oc','osfstatus','lineno','bidv1','bidv2','bidv3','bidv4','bidv5','askv1','askv2','askv3','askv4','askv5','dbuyv1','dsellv1','contract','tbuyv','tsellv','ybuyre','ysellre','tbuyre','tsellre','settle','TXF_c','MXF_c','TXOc_K','TXOp_K','TXF_HRV_Day','MXF_HRV_Day','sumvol','5min','10min','classify1','classify2','strategy','delvol','vol1','vol2','odrtype','excd','aftxcd','aftshr','mtfshr','mtf_shr','buy_cnt','sell_cnt','rising','falling','osfbuycnt','osfbuyqty','osfsellcnt','osfsellqty','mtfcnt','mtfqty','mtfamt']
        double = ['price','bid1','bid2','bid3','bid4','bid5','ask1','ask2','ask3','ask4','ask5','dbuy1','dsell1','high','low','rv','lnvolume','1/p','mid','price1','price2','points','strike1','strike2','strike']

        if daytime == 10:
            i_str = ".date(2006-01-02)"
        if daytime == 19:
            i_str = ".date(2006-01-02 15:04:05)"
        if daytime > 19:
            i_str = ".date(2006-01-02 15:04:05.000000)"
        aa = {"daytime":("daytime"+i_str)}
        # Double
        double = zippo(double,doub,".double()",aa)
        # String
        string = zippo(string,stri,".string()",aa)
        # int
        intt = zippo(intt,INT,".int32()",aa)
        data.rename(columns=aa,inplace=True)
        return data
    # 2021.3.16 lin修改 self縮短code以及文字format 
    def mongoimport(self,db,collection,path):
        if self.user!=0:
            os.system(f"mongoimport --username={self.user} --password={self.pwd} --authenticationDatabase=admin --host={self.ip} --port={self.port} --db={db} --collection={collection} --type=csv --file={path} --headerline --columnsHaveTypes")
        else:
            os.system(f"mongoimport --host={self.ip} --port={self.port} --db={db} --collection={collection} --type=csv --file={path} --headerline --columnsHaveTypes")
        return path
