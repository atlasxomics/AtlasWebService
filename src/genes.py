##################################################################################
### Module : genes.py
### Description : Gene API 
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
import csv
from . import utils 
import scanpy as sc
import numpy as np 
import jwt

class GeneAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.qc_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['studies.qc']['table_name'])
        self.storageApi=self.auth.app.config['SUBMODULES']['StorageAPI']
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):

#### Gene Spatial & Umap
        @self.auth.app.route('/api/v1/genes/gms',methods=['POST'])
        @self.auth.login_required
        def _getGeneMotifSpatial():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res=self.getGeneMotifSpatial(req)
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
        @self.auth.app.route('/api/v1/genes/gms/<token>',methods=['POST'])
        def _getGeneMotifSpatialByToken(token):
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.getGeneMotifSpatialByToken(token)
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
        @self.auth.app.route('/api/v1/genes/expressions',methods=['POST'])
        @self.auth.login_required
        def _getGeneExpressions():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res=self.getGeneExpressions(req, u, g)
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

        @self.auth.app.route('/api/v1/genes/expressions/<token>',methods=['POST'])
        def _getGeneExpressionsByToken(token):
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.getDataByToken(token)
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

        @self.auth.app.route('/api/v1/genes/generate_link',methods=['POST'])
        @self.auth.login_required 
        def _generateLink():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res=self.generateLink(req, u, g)
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

        @self.auth.app.route('/api/v1/genes/decode_link/<link>',methods=['GET'])
        @self.auth.admin_required 
        def _decodeLink(link):
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res=self.decodeLink(link, u, g)
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


        # @self.auth.app.route('/api/v1/genes/spatial',methods=['POST'])
        # @self.auth.admin_required 
        # def _getGeneSpatial():
        #     sc=200
        #     res=None
        #     req=request.get_json()
        #     try:
        #         u,g=current_user
        #         res=self.getGeneSpatial(req, u, g)
        #     except Exception as e:
        #         sc=500
        #         exc=traceback.format_exc()
        #         res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
        #         self.auth.app.logger.exception(res['msg'])
        #     finally:
        #         resp=Response(json.dumps(res),status=sc)
        #         resp.headers['Content-Type']='application/json'
        #         self.auth.app.logger.info(utils.log(str(sc)))
        #         return resp  
    def getGeneMotifSpatial(self,req):
      files = ['gene', 'motif', 'spatial']
      container = {}
      for i in files:
        name = self.getFileObject(self.bucket_name, req[i])
        with open(name, 'r') as read_obj:
          csv_reader = csv.reader(read_obj)
          if i == 'gene' or i == 'motif':
            container[i] = {}
            for row in csv_reader:
              container[i][row[0]] = row[1:]
          else:
            container[i] = list(csv_reader)
      return container
    def getGeneMotifSpatialByToken(self, token):
      req = self.decodeLink(token, None, None)
      gene = req['args'][1]
      motif = req['args'][2]
      spatial = req['args'][0]
      return self.getGeneMotifSpatial({'gene': gene, 'motif': motif, 'spatial': spatial})
    def getGeneExpressions(self,req, u, g): ## gene expression array 
        if "filename" not in req: return utils.error_message("No filename is provided",500)
        filename = req['filename']
        downloaded_filename = self.getFileObject(self.bucket_name, filename)
        adata=sc.read(downloaded_filename)
        return list(adata.var_names)

    def getDataByToken(self,token):
        req = self.decodeLink(token, None, None)
        filename = req['args'][0]
        payload = {
            'filename': filename
        }
        return self.getGeneExpressions(payload, None, None)

    def generateLink(self,req,u,g):
        secret=self.auth.app.config['JWT_SECRET_KEY']
        encoded_payload = jwt.encode(req, secret, algorithm='HS256')
        return {'encoded': encoded_payload}

    def decodeLink(self,link,u,g):
        secret=self.auth.app.config['JWT_SECRET_KEY']
        return jwt.decode(link, secret,algorithms=['HS256'])

    # def getGeneSpatial(self, req, u, g): ## spatial plot
    #     if "filename" not in req: return utils.error_message("No filename is provided",500)
    #     filename = req['filename']
    #     requested_genes = req['genes']
    #     downloaded_filename = self.getFileObject(self.bucket_name, filename)
    #     computed_filename= Path(downloaded_filename).parent.joinpath(Path(downloaded_filename).stem+"_computed.h5ad").__str__()
    #     adata=None
    #     if Path(computed_filename).exists():
    #         adata=sc.read(computed_filename)
    #     else:      
    #         adata=sc.read(downloaded_filename)
    #         sc.pp.calculate_qc_metrics(adata, inplace=True)
    #         sc.pp.pca(adata)
    #         sc.pp.neighbors(adata)
    #         sc.tl.umap(adata)
    #         sc.tl.leiden(adata,key_added="clusters")
    #         adata.write(computed_filename)
    #     out={}
    #     out['clusters']=adata.obs['clusters'].tolist()
    #     out['coordinates']=adata.obsm['spatial'].tolist()
    #     out['coordinates_umap']=adata.obsm['X_umap'].tolist()
    #     out['genes']={}
    #     out['genes_summation']=np.zeros(len(out['coordinates']))
    #     for g_exp in requested_genes:
    #         out['genes'][g_exp]= list(map(lambda x: x[0],adata[:,g_exp].X.todense().tolist()))
    #     for k,v in out['genes'].items():
    #         out['genes_summation']+=np.array(v)
    #     out['genes_summation']=out['genes_summation'].tolist()
    #     return out
    
    def getFileObject(self,bucket_name,filename):
        _,tf=self.storageApi.checkFileExists(bucket_name,filename)
        temp_outpath=self.tempDirectory.joinpath(filename)
        if temp_outpath.exists(): return str(temp_outpath)
        temp_outpath.parent.mkdir(parents=True, exist_ok=True)
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.storageApi.aws_s3.download_fileobj(bucket_name,filename,f)
            f.close()

        return str(temp_outpath)
