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
# from turtle import pd
from flask import request, Response , send_from_directory
from flask_jwt_extended import jwt_required,get_jwt_identity,current_user
from werkzeug.utils import secure_filename
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
        user = self.auth.app.config['SLIMS_USERNAME']
        passw = self.auth.app.config['SLIMS_PASSWORD']

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):
##### SLIMS
        @self.auth.app.route('/api/v1/dataset/slimstest_list_runids',methods=['GET'])
        @self.auth.login_required
        def _getSlimsRunsList():
            #run_id=request.args.get('run_id',type=str)
            cntn_type=request.args.get('cntn_type', default="NGS Library",type=str)
            u,g=current_user
            data=''
            sc=200
            res=None
            try:
                endpoint = "https://slims.atlasxomics.com/slimsrest/rest/Content"
                user = self.auth.app.config['SLIMS_USERNAME']
                passw = self.auth.app.config['SLIMS_PASSWORD']
                if not u:
                    raise Exception("User is empty")
                if not g:
                    raise Exception("Group is empty")
                if 'admin' in g:
                    response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw))
                    data = response.json()
                    data = data['entities']
                else:
                    data_list = []
                    for group in g:
                        try:
                            payload = {'cntn_cf_source': group}
                            response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw), params = payload)
                            data = response.json()
                            if data['entities']:
                                data_list.append(data['entities'])
                            else:
                                print(" \'entities\' is empty")
                        except Exception as e:
                            print(f"{group} cannot be queried. Original error: {e}")
                    data = [i for data in data_list for i in data]
                res = []
                meta=["cntn_cf_runId", "cntn_cf_source"]
                for i in data:
                    sub_dict = {k['title']: k['value'] for k in i['columns'] if k['name'] in meta and k['value'] is not None}
                    sub_dict['pk'] = i['pk']
                    if 'Run Id' in sub_dict.keys():
                        res.append(sub_dict)
                    else:
                        continue
            except Exception as e: 
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                return resp

        @self.auth.app.route('/api/v1/dataset/slimstest_runid',methods=['GET'])
        @self.auth.login_required
        def _getSlimsRun():
            sc=200
            res=None
            pd_dict = {}
            flow_results = {}
            try:
                # obtaining the parameters of the data being passed from client on AtlasWeb
                run_id = request.args.get('run_id', type=str)
                # creating payload to pass to SLIMS REST API
                pd_dict = self.getSlimsMeta_runID(run_id)
                tissue_pk = pd_dict.get("tissue_slide_pk", None)
                if tissue_pk:
                    flow_results = self.getFLowResults(tissue_pk)
                    pd_dict.update(flow_results)
            except Exception as e:
                print(e)
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e),exc))
                print(res)
            finally:
                resp = Response(json.dumps(pd_dict),status=sc)
                resp.headers['Content-Type']='application/json'
                return resp        

        @self.auth.app.route('/api/v1/dataset/slimstest_ngs',methods=['GET'])
        @jwt_required()
        def _getSlimsNGS():
            sc=200
            res=None
            try:
                cntn_type = request.args.get('cntn_type', default="NGS Library",type=str)
                ngs_id = request.args.get('ngs_id', type=str)
                payload = {'cntp_name': cntn_type, 'cntn_id': ngs_id}
                meta = ["cntn_cf_runId", "cntn_id", "cntn_cf_source", 
                        "cntn_cf_fk_tissueType", "cntn_cf_fk_organ", 
                        "cntn_cf_fk_species", "cntn_cf_fk_workflow"]
                res = self.getSlimsMeta(payload, meta)
            except Exception as e: 
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e),exc))
            finally:
                resp = Response(json.dumps(res),status=200)
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
#### SLIMSa

    def to_dict(self, lis):
        dic = { lis[i]["name"] : lis[i] for i in range(len(lis)) }
        return dic

    def getSlimsMeta_runID(self, run_id):
        meta = ["cntn_cf_runId", "cntn_cf_source", "cntn_cf_fk_tissueType", 
        "cntn_cf_fk_organ", "cntn_cf_fk_species", 
        "cntn_cf_fk_workflow", "cntn_id", "cntn_cf_fk_chipB", "cntn_cf_fk_barcodeOrientation",
        "cntn_cf_experimentalCondition", "cntn_cf_sampleId", "cntn_cf_fk_tissueType", "cntn_cf_fk_epitope"
        ]
        endpoint = "https://slims.atlasxomics.com/slimsrest/rest/Content"
        user = self.auth.app.config['SLIMS_USERNAME']
        passw = self.auth.app.config['SLIMS_PASSWORD']
        payload = { "cntn_cf_runId": run_id, "cntn_fk_contentType": 42}
        response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw), params = payload)
        tissue_slidedata = response.json()

        cols = tissue_slidedata["entities"][0]["columns"]

        display_vals = set(( "cntn_cf_fk_tissueType", "cntn_cf_fk_organ", "cntn_cf_fk_species", "cntn_cf_fk_workflow", "cntn_cf_fk_barcodeOrientation", "cntn_cf_fk_epitope" ))
        reformatted_strings = set(("cntn_cf_fk_organ", "cntn_cf_fk_tissueType", "cntn_cf_fk_species", "cntn_cf_fk_workflow"))
        resp = {}
        col_dict = self.to_dict(cols)
        resp["tissue_slide_pk"] = tissue_slidedata["entities"][0]["pk"]
        for col_name in meta:
            col_content = col_dict.get(col_name, {})
            if col_name in display_vals and "displayValue" in col_content.keys():
                disp_val = col_content["displayValue"]
                if disp_val and col_name in reformatted_strings:
                    disp_val = self.format_string(disp_val)
                resp[col_name] = disp_val 
            else:
                resp[col_name] = col_content.get("value")
        
        return resp

    def list_to_string(self, lis):
        string = ''
        for inx in range(len(lis)):
            string += str(lis[inx])
            if inx != len(lis) - 1:
                string += ","
        return string

    def format_string(self, orig):
        val = orig.strip()
        replaced = val.replace(' ', '_')
        lower = replaced.lower()
        return lower 

    def getFLowResults(self, pk):
        user = self.auth.app.config['SLIMS_USERNAME']
        passw = self.auth.app.config["SLIMS_PASSWORD"]
        endpoint = "https://slims.atlasxomics.com/slimsrest/rest/Result"
        payload = {
            "rslt_fk_content": pk,
            "rslt_fk_test": 33
        }
        response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw), params=payload)
        data = response.json()
        print(data)
        final_flow_results = {}
        if len(data) != 0:
            flow_tests = []
            for i in range(len(data["entities"])):
                cols = data["entities"][i]["columns"]
                d = self.to_dict(cols)
                current_test = {}
                current_test["blocks"] = self.list_to_string(d["rslt_cf_fk_blocks"]["value"])
                current_test["leak"] = d["rslt_cf_leak"]["value"]
                current_test["crosses"] = self.list_to_string(d["rslt_cf_fk_leaks"]["value"])
                current_test["comments"] = d["rslt_comments"]["value"]
                current_test["expr_step"] = d["rslt_fk_experimentRunStep"]["value"]

                flow_tests.append(current_test)

            endpoint2 = "https://slims.atlasxomics.com/slimsrest/rest/ExperimentRunStep"
            if len(flow_tests) != 0:
                test = flow_tests[0]
                payload3 = {
                    "xprs_pk": test["expr_step"],
                }
                response = requests.get(endpoint2, auth=HTTPBasicAuth(user, passw), params=payload3)
                data3 = response.json()

                isA = False
                cols = data3["entities"][0]["columns"]
                for k in range(len(cols)):
                    name = cols[k]["name"]
                    if name == "rslt_fk_experimentRunStep":
                        if cols[k]["value"] == 729:
                            isA = True
                post_1 = "_flowA"
                post_2 = "_flowB"
                flow_tests[0].pop("expr_step")
                flow_tests[1].pop("expr_step")
                if not isA:
                    temp = post_1
                    post_1 = post_2
                    post_2 = temp
                for key in flow_tests[0].keys():
                    final_flow_results[key + post_1] = flow_tests[0][key]
                for key in flow_tests[1].keys():
                    final_flow_results[key + post_2] = flow_tests[1][key]
        
        return final_flow_results

    def getSlimsMeta(self,payload,meta):
            endpoint = "https://slims.atlasxomics.com/slimsrest/rest/Content"
            user = self.auth.app.config['SLIMS_USERNAME']
            passw = self.auth.app.config['SLIMS_PASSWORD']
            response = requests.get(endpoint, auth=HTTPBasicAuth(user, passw), params = payload)
            data = response.json()
            pd_dict = []
            for i in data['entities']:
                sub_dict = {k['title']: (k['displayValue'] if 'displayValue' in k.keys() else k['value']) for k in i['columns'] if k['name'] in meta}
                sub_dict['pk'] = i['pk']
                pd_dict.append(sub_dict)
            return(pd_dict)

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




