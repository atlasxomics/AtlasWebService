##################################################################################
### Module : database.py
### description : MongoDB class
### Requirements : pymongo
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/02
### Copyrighted reserved by AtlasXomics
##################################################################################

import datetime
from functools import wraps
from enum import IntEnum
import os 
import json
import uuid
import traceback

## non standard libraries
from pymongo import MongoClient
from bson.objectid import ObjectId

## application libraries
from . import utils

class MongoDB(object):
    def __init__(self,auth):

        self.auth=auth
        self.client=None
        self.host=self.auth.app.config['MONGO_HOST']
        self.port=self.auth.app.config['MONGO_PORT']
        self.username=self.auth.app.config['MONGO_INITDB_ROOT_USERNAME']
        self.password=self.auth.app.config['MONGO_INITDB_ROOT_PASSWORD']
        self.db=None
        self.initialize()
        self.initializeTables()
        self.test()

    def initialize(self):
        try:
            self.client=MongoClient(self.host,self.port,username=self.username,password=self.password)
            self.db=self.client[self.auth.app.config['MONGO_DBNAME']]
        except Exception as e:
            exc=traceback.format_exc()
            self.auth.app.logger.exception("Exception in initializing mongodb: {} {}".format(str(e),exc))
        #existing_tables = [t.name for t in self.client.tables.all()]

    def initializeTables(self):
        table_info=self.auth.app.config['DATA_TABLES']
        for k,v in table_info.items():
            tablename=v['table_name']
            ### index generation
            if "indexes" in v:
                for idx in v['indexes']:
                    try:
                        self.db[tablename].create_index(idx,unique=True)
                    except Exception as e:
                        continue

    def getDatabase(self):
        return self.db

    def getTable(self,table_name):
        return self.db[table_name]

    def test(self):
        q=self.client['admin']['system.users'].find({})
        print(list(q))

