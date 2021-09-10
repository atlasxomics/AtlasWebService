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
import copy
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/wafers',methods=['GET'])
        @jwt_required()
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  


        @self.auth.app.route('/api/v1/dataset/wafertrace',methods=['GET'])
        @jwt_required()
        def _getWaferTrace():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            param_dbits_only=request.args.get('dbits_only',default="true",type=str)
            dbits_only=True
            if param_dbits_only.lower() != 'true': dbits_only=False
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getWaferTrace(param_filter,param_options,u,g,dbits_only=dbits_only)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  



        @self.auth.app.route('/api/v1/dataset/wafers/<wafer_id>/usage',methods=['GET'])
        @jwt_required()
        def _getWaferUsage(wafer_id):
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            param_filter.update({'wafer_id':wafer_id})
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getWafers_Usage(wafer_id,param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp             

        @self.auth.app.route('/api/v1/dataset/wafers/<wafer_id>/chips',methods=['GET'])
        @jwt_required()
        def _getWaferChips(wafer_id):
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            param_filter.update({'wafer_id':wafer_id})
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getWafers_Usage(wafer_id,param_filter,param_options,u,g)
                res=[ k for k,v in res.items()]
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp             

        @self.auth.app.route('/api/v1/dataset/wafers/<wafer_id>/dbits',methods=['GET'])
        @jwt_required()
        def _getDbitRuns(wafer_id):
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            param_filter.update({'wafer_id':wafer_id})
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                data= self.getWafers_Usage(wafer_id,param_filter,param_options,u,g)
                res=[]
                for k,v in data.items(): res+=v 
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/chips',methods=['GET'])
        @jwt_required()
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

#### Study
        @self.auth.app.route('/api/v1/dataset/studies',methods=['POST'])
        @self.auth.admin_required 
        def _addStudies():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res= self.addStudies(req,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/studies',methods=['GET'])
        @jwt_required()
        def _getStudies():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getStudies(param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/dbits',methods=['GET'])
        @jwt_required()
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
                res=utils.error_message("Error : {} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  
###### methods

    def getWafers_Usage(self,wafer_id,fltr,options,username,groups):
        chips_tablename=self.auth.app.config['DATA_TABLES']['chips']['table_name']
        dbits_tablename=self.auth.app.config['DATA_TABLES']['dbits']['table_name']

        chips_table=self.datastore.getTable(chips_tablename)
        dbits_table=self.datastore.getTable(dbits_tablename)
        chip_list=list(chips_table.find({'waferid':wafer_id}))
        
        usage_map={}
        for chip in chip_list:
            chip_id=chip['chip_id']
            dbit_list=list(dbits_table.find({'$or':[{'chip_a_id':chip_id},{'chip_b_id':chip_id} ]},options))
            usage_map[chip_id]=dbit_list
        return usage_map

    def getWaferTrace(self,fltr,options,username,groups,dbits_only):
        wafer_tablename=self.auth.app.config['DATA_TABLES']['wafers']['table_name']
        wafer_table=self.datastore.getTable(wafer_tablename)
        wafer_list=wafer_table.find(fltr)
        
        wafer_tracks={}
        for w in wafer_list:
            temp=self.getWafers_Usage(w['wafer_id'],{},options,username,groups)
            wafer_tracks[w['wafer_id']]=temp

        filtered_wafer_tracks=copy.deepcopy(wafer_tracks)
        if dbits_only:
            ## filter out all the wafers, chips which doesn't have any dbits
            for w, wv in wafer_tracks.items():
                dbits_count=0
                for c, cv in wv.items():
                    if len(cv) < 1 : 
                        del filtered_wafer_tracks[w][c]
                    else:
                        dbits_count+=1
                if dbits_count<1 : del filtered_wafer_tracks[w]

        return filtered_wafer_tracks

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

    def addStudies(self,payload,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['studies']['table_name']
        table=self.datastore.getTable(tablename)
        res=table.insert_many(payload)
        print(res)
        return utils.result_message("Upload succeeded")

    def getStudies(self,fltr,options,username,groups):
        tablename=self.auth.app.config['DATA_TABLES']['studies']['table_name']
        table=self.datastore.getTable(tablename)
        res=list(table.find(fltr,options))
        return res 