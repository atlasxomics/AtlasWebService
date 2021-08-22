##################################################################################
### Module : dataset.py
### Description : Dataset API 
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/22
### Copyrights reserved by AtlasXomics
##################################################################################

### API
from flask import request, Response , send_from_directory
from flask_jwt_extended import jwt_required,get_jwt_identity,current_user
from werkzeug.utils import secure_filename
# import miscellaneous modules
import os
import io
import uuid
import time
import traceback
import sys
import json
from pathlib import Path
import random
import datetime
import shutil

from . import utils 

class DatasetAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):

#### WAFERS
        @self.auth.app.route('/api/v1/dataset/wafers',methods=['POST'])
        @self.auth.admin_required 
        def _addWafers():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res= self.addWafers(req,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/wafers',methods=['GET'])
        @self.auth.admin_required 
        def _getWafers():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getWafers(param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

#### CHIPS
        @self.auth.app.route('/api/v1/dataset/chips',methods=['POST'])
        @self.auth.admin_required 
        def _addChips():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res= self.addChips(req,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/chips',methods=['GET'])
        @self.auth.admin_required 
        def _getChips():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getChips(param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  


#### DBiT Runs
        @self.auth.app.route('/api/v1/dataset/dbits',methods=['POST'])
        @self.auth.admin_required 
        def _addDBiT():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res= self.addDBiT(req,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/dbits',methods=['GET'])
        @self.auth.admin_required 
        def _getDBiT():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getDBiT(param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc))
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  
###### methods
    def addWafers(self,payload,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['wafers']['table_name']
        table=self.datastore.getTable(tablename)
        res=table.insert_many(payload)
        print(res)
        return utils.result_message("Upload succeeded")

    def getWafers(self,fltr,options,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['wafers']['table_name']
        table=self.datastore.getTable(tablename)
        res=list(table.find(fltr,options))
        return res 

    def addChips(self,payload,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['chips']['table_name']
        table=self.datastore.getTable(tablename)
        res=table.insert_many(payload)
        print(res)
        return utils.result_message("Upload succeeded")

    def getChips(self,fltr,options,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['chips']['table_name']
        table=self.datastore.getTable(tablename)
        res=list(table.find(fltr,options))
        return res 

    def addDBiT(self,payload,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['dbits']['table_name']
        table=self.datastore.getTable(tablename)
        res=table.insert_many(payload)
        print(res)
        return utils.result_message("Upload succeeded")

    def getDBiT(self,fltr,options,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['dbits']['table_name']
        table=self.datastore.getTable(tablename)
        res=list(table.find(fltr,options))
        return res 
