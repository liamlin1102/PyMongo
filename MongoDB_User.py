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
    #資料庫查看
    def show_dbs(self):
        get=self.client.list_database_names()
        get=pd.DataFrame(get)
        get.columns = ['dbs']
        return get
    def show_collections(self,db):
        link = self.client[db]
        get = sorted(link.list_collection_names(session=None))
        get= pd.DataFrame(get)
        get.columns = ['collections']
        return get
    #資料庫資料獲取
    def load_data(self,db,collection,key,field):
        link=self.client[db][collection]
        data = pd.DataFrame(list(link.find(key,field)))
        return data
    def load_data_one(self,db,collection,key,field):
        link=self.client[db][collection]
        if field =={}:
            field = {"_id":0}
        dataj=link.find_one(key,field)
        if not dataj is None:
            data = pd.DataFrame.from_dict(dataj,orient='index').T
        else: data = pd.DataFrame(columns=["acc"],data=[0])
        return data
    def load_data_distinct(self,db,collection,key,field):
        link=self.client[db][collection]
        data = pd.DataFrame(list(link.distinct(key,field)))
        return data
    #資料庫查詢名目管理
    def create_key(self,db,collection,title):
        link=self.client[db][collection]
        get = link.create_index([(title[0], pymongo.ASCENDING)],background=True) #ASCENDING 升序 DESCENDING 降序
        return get
    def create_keys(self,db,collection,title):
        link=self.client[db][collection]
        a=0
        b=[]
        while a<len(title):
            b.append((title[a], pymongo.ASCENDING))
            a+=1
        get = link.create_index(b,background=True) #ASCENDING 升序 DESCENDING 降序
        return get
    def get_keys(self,db,collection):
        link=self.client[db][collection]
        get = link.index_information()
        return get
    def del_keys(self,db,collection):
        link=self.client[db][collection]
        get = link.drop_indexes()
        return get
    #     2021.3.16 Lin新增 del_key 2021.3.17 huang finish problem(del_key )
    def del_key(self,db,collection,title):
        link=self.client[db][collection]
        data = "_1_"
        get = link.drop_index(data.join(title)+"_1")
        return get
    def del_collection(self,db,collection):
        link=self.client[db]
        get = link.drop_collection(collection)
        return get
    def del_db(self,db):
        get = self.client.drop_database(db)
        return get
    # 2021.3.17 lin&huang change name and code
    def insert_DataFrame(self,db,collection,data):
        link=self.client[db][collection]
        if not isinstance(data, dict):
            records = data.to_dict("records")
        get=link.insert_many(records)
        return get
