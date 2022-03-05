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
import yaml
from . import utils
import requests
from requests.auth import HTTPBasicAuth

class DatasetAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.wafers_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['metadata.wafers']['table_name'])
        self.chips_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['metadata.chips']['table_name'])
        self.dbits_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['metadata.dbits']['table_name'])
        self.qc_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['studies.qc']['table_name'])
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):

#### SLIMS
        @self.auth.app.route('/api/v1/dataset/slimstest',methods=['GET'])
        @jwt_required()
        def _getSlims():
            run_id=request.args.get('run_id',type=str)
            cntn_type=request.args.get('cntn_type', default="Tissue slide",type=str)
            data = ''
            try:
                endpoint = "https://slims.atlasxomics.com/slimsrest/rest/Content"
                user = self.auth.app.config['SLIMS_USERNAME']
                passw = self.auth.app.config['SLIMS_PASSWORD']
                #cntn_type = 'Tissue slide'
                #run_id = "D210"
                payload = {'cntn_cf_runId': run_id, 'cntp_name': cntn_type}
                response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw), params = payload)
            
                print(response.url)
                print(response.encoding)
                data = response.json()
            except requests.exceptions.RequestException as e: 
                print(str(e))

            pd_dict = []
            meta=["Run Id", "Id", "Source", "Tissue type", "Organ", "Species", "Assay"]
            for i in data['entities']:
                sub_dict = {k['title']: (k['displayValue'] if 'displayValue' in k.keys() else k['value']) for k in i['columns'] if k['title'] in meta}
                sub_dict['pk'] = i['pk']
                pd_dict.append(sub_dict)
            resp=Response(json.dumps(pd_dict),status=200)
            resp.headers['Content-Type']='application/json'
            
            return resp         

#### WAFERS
        @self.auth.app.route('/api/v1/dataset/wafers',methods=['POST','PUT'])
        @self.auth.admin_required 
        def _addWafers():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                if request.method=='POST':
                    res= self.addWafers(req,u,g)
                elif request.method=='PUT':
                    res= self.updateWafers(req,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/wafers/upload',methods=['POST'])
        @self.auth.admin_required 
        def _uploadWafers():
            sc=200
            res=None
            try:
                u,g=current_user
                f=request.files['file']
                filename=f.filename
                bucket_name=self.bucket_name
                output_filename=Path(filename).name
                if 'bucket_name' in request.values:
                    if request.values['bucket_name']:
                        bucket_name=request.values['bucket_name']
                if 'output_filename' in request.values:
                    if request.values['output_filename']: 
                        output_filename=request.values['output_filename']
                        output_filename=Path(output_filename)
                        try:
                            output_filename=output_filename.relative_to('/')
                        except:
                            pass 
                        output_filename=output_filename.__str__()
                payload={'uploaded_by':u.username}
                if 'meta' in request.values:
                    try:
                        payload.update(json.loads(request.values['meta']))
                    except:
                        pass
                
                res= self.uploadWaferFile(bucket_name,f,output_filename,meta=payload)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                print(exc)
                res=utils.error_message("{} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/wafers',methods=['GET','DELETE'])
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
                if request.method=='GET':
                    res= self.getWafers(param_filter,param_options,u,g)
                elif request.method=='DELETE':
                    res= self.deleteWafers(param_filter,param_options,u,g)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
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

        @self.auth.app.route('/api/v1/dataset/chips/upload',methods=['POST'])
        @self.auth.admin_required 
        def _uploadChips():
            sc=200
            res=None
            try:
                u,g=current_user
                f=request.files['file']
                filename=f.filename
                bucket_name=self.bucket_name
                output_filename=Path(filename).name
                if 'bucket_name' in request.values:
                    if request.values['bucket_name']:
                        bucket_name=request.values['bucket_name']
                if 'output_filename' in request.values:
                    if request.values['output_filename']: 
                        output_filename=request.values['output_filename']
                        output_filename=Path(output_filename)
                        try:
                            output_filename=output_filename.relative_to('/')
                        except:
                            pass 
                        output_filename=output_filename.__str__()
                payload={'uploaded_by':u.username}
                if 'meta' in request.values:
                    try:
                        payload.update(json.loads(request.values['meta']))
                    except:
                        pass
                
                res= self.uploadChipFile(bucket_name,f,output_filename,meta=payload)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                print(exc)
                res=utils.error_message("Error while uploading : {} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/chips',methods=['POST','PUT'])
        @self.auth.admin_required 
        def _addChips():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                if request.method=='POST':
                    res= self.addChips(req,u,g)
                elif request.method=='PUT':
                    res= self.updateChips(req,u,g)
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

        @self.auth.app.route('/api/v1/dataset/chips',methods=['GET','DELETE'])
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
                if request.method=='GET':
                    res= self.getChips(param_filter,param_options,u,g)
                elif request.method=='DELETE':
                    res= self.deleteChips(param_filter,param_options,u,g)
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
        @self.auth.app.route('/api/v1/dataset/dbits',methods=['POST','PUT'])
        @self.auth.admin_required 
        def _addDBiT():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                if request.method=='POST':
                    res= self.addDBiT(req,u,g)
                elif request.method=='PUT':
                    res= self.updateDBiT(req,u,g)
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

        @self.auth.app.route('/api/v1/dataset/dbits/upload',methods=['POST'])
        @self.auth.admin_required 
        def _uploadDbits():
            sc=200
            res=None
            try:
                u,g=current_user
                f=request.files['file']
                filename=f.filename
                bucket_name=self.bucket_name
                output_filename=Path(filename).name
                if 'bucket_name' in request.values:
                    if request.values['bucket_name']:
                        bucket_name=request.values['bucket_name']
                if 'output_filename' in request.values:
                    if request.values['output_filename']: 
                        output_filename=request.values['output_filename']
                        output_filename=Path(output_filename)
                        try:
                            output_filename=output_filename.relative_to('/')
                        except:
                            pass 
                        output_filename=output_filename.__str__()
                payload={'uploaded_by':u.username}
                if 'meta' in request.values:
                    try:
                        payload.update(json.loads(request.values['meta']))
                    except:
                        pass
                
                res= self.uploadDbitFile(bucket_name,f,output_filename,meta=payload)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                print(exc)
                res=utils.error_message("Error while uploading : {} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        @self.auth.app.route('/api/v1/dataset/dbits',methods=['GET','DELETE'])
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
                if request.method=='GET':
                    res= self.getDBiT(param_filter,param_options,u,g)
                elif request.method=='DELETE':
                    res= self.deleteDBiT(param_filter,param_options,u,g)
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
#### Study.QC
        @self.auth.app.route('/api/v1/dataset/qc',methods=['POST','PUT'])
        @self.auth.admin_required 
        def _addQc():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                if request.method=='POST':
                    res= self.addQc(req,u,g)
                elif request.method=='PUT':
                    res= self.updateQc(req,u,g)
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


        @self.auth.app.route('/api/v1/dataset/qc',methods=['GET'])
        @jwt_required()
        def _getQc():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.getQc(param_filter,param_options,u,g)
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

        @self.auth.app.route('/api/v1/dataset/qc',methods=['DELETE'])
        @self.auth.admin_required
        def _deleteQc():
            sc=200
            res=None
            param_filter=json.loads(request.args.get('filter',default="{}",type=str))
            param_options=request.args.get('options',default=None,type=str)
            if param_options is not None:
                param_options=json.loads(param_options)
            try:
                u,g=current_user
                res= self.deleteQc(param_filter,param_options,u,g)
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

#### wafers
    def getWafers_Usage(self,wafer_id,fltr,options,username,groups):
        chips_table=self.chips_table
        dbits_table=self.dbits_table
        chip_list=list(chips_table.find({'wafer_id':wafer_id}))
        
        usage_map={}
        for chip in chip_list:
            chip_id=chip['chip_id']
            dbit_list=list(dbits_table.find({'$or':[{'chip_a_id':chip_id},{'chip_b_id':chip_id} ]},options))
            usage_map[chip_id]=dbit_list
        return usage_map

    def getWaferTrace(self,fltr,options,username,groups,dbits_only):
        wafer_table=self.wafers_table
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
        table=self.wafers_table
        for idx,doc in enumerate(payload):
            payload[idx]['_id']=doc['wafer_id']
        res=table.insert_many(payload)
        return utils.result_message("Upload succeeded")

    def updateWafers(self,payload,username,groups):
        table=self.wafers_table
        mc=0
        upsids=[]
        for idx,doc in enumerate(payload):
            r=table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def getWafers(self,fltr,options,username,groups):
        table=self.wafers_table
        res=list(table.find(fltr,options))
        return res 

    def deleteWafers(self,fltr,options,username,groups):
        table=self.wafers_table
        res=table.delete_many(fltr)
        dc=res.deleted_count
        return utils.result_message({"deleted_count":dc})

    def uploadWaferFile(self,bucket_name,fileobj,output_key,meta={}):
        temp_outpath=self.tempDirectory.joinpath("{}_{}".format(utils.get_uuid(),Path(fileobj.filename).name))
        fileobj.save(str(temp_outpath))
        payload=utils.make_dataset_from_csv(temp_outpath,mandatory_keys=['wafer_id'])
        mc=0
        upsids=[]
        # return utils.result_message({'paylod':payload})
        for doc in payload:
            r=self.wafers_table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })
### Chips
    def uploadChipFile(self,bucket_name,fileobj,output_key,meta={}):
        temp_outpath=self.tempDirectory.joinpath("{}_{}".format(utils.get_uuid(),Path(fileobj.filename).name))
        fileobj.save(str(temp_outpath))
        payload=utils.make_dataset_from_csv(temp_outpath,mandatory_keys=['chip_id'])
        mc=0
        upsids=[]
        for doc in payload:
            r=self.chips_table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def addChips(self,payload,username,groups):
        table=self.chips_table
        for idx,doc in enumerate(payload):
            payload[idx]['_id']=doc['chip_id']
        res=table.insert_many(payload)
        return utils.result_message("Upload succeeded")

    def updateChips(self,payload,username,groups):
        table=self.chips_table
        mc=0
        upsids=[]
        for doc in payload:
            r=table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def getChips(self,fltr,options,username,groups):
        table=self.chips_table
        res=list(table.find(fltr,options))
        return res 

    def deleteChips(self,fltr,options,username,groups):
        table=self.chips_table
        res=table.delete_many(fltr)
        dc=res.deleted_count
        return utils.result_message({"deleted_count":dc})
### dbits

    def addDBiT(self,payload,username,groups):
        table=self.dbits_table
        for idx,doc in enumerate(payload):
            payload[idx]['_id']=doc['run_id']
        res=table.insert_many(payload)
        return utils.result_message("Upload succeeded")

    def updateDBiT(self,payload,username,groups):
        table=self.dbits_table
        mc=0
        upsids=[]
        for doc in payload:
            r=table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def uploadDbitFile(self,bucket_name,fileobj,output_key,meta={}):
        temp_outpath=self.tempDirectory.joinpath("{}_{}".format(utils.get_uuid(),Path(fileobj.filename).name))
        fileobj.save(str(temp_outpath))
        payload=utils.make_dataset_from_csv(temp_outpath,mandatory_keys=['run_id'])
        mc=0
        upsids=[]
        # return utils.result_message({'paylod':payload})
        for doc in payload:
            r=self.dbits_table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def getDBiT(self,fltr,options,username,groups):
        table=self.dbits_table
        res=list(table.find(fltr,options))
        return res 

    def deleteDBiT(self,fltr,options,username,groups):
        table=self.dbits_table
        res=table.delete_many(fltr)
        dc=res.deleted_count
        return utils.result_message({"deleted_count":dc})

### qc
    def addQc(self,payload,username,groups):
        table=self.qc_table
        for idx,doc in enumerate(payload):
            payload[idx]['_id']=doc['id']
        res=table.insert_many(payload)
        return utils.result_message("Upload succeeded")

    def updateQc(self,payload,username,groups):
        table=self.qc_table
        for idx,doc in enumerate(payload):
            payload[idx]['_id']=doc['id']
        mc=0
        upsids=[]
        for doc in payload:
            r=table.replace_one({"_id": doc["_id"]},doc,upsert=True)
            mc+=r.modified_count
            upsids+=[r.upserted_id]
        return utils.result_message({ 'modified_count':mc, 'upserted_ids':upsids })

    def getQc(self,fltr,options,username,groups):
        table=self.qc_table
        res=list(table.find(fltr,options))
        return res 

    def deleteQc(self,fltr, options, username, groups):
        table=self.qc_table
        res=table.delete_many(fltr,options)
        return utils.result_message({"deleted" : res.deleted_count})      




