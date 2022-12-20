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
import gzip
import re
import jwt
import boto3
from botocore.exceptions import ClientError

class GeneAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.qc_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['studies.qc']['table_name'])
        self.aws_s3=boto3.client('s3')
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):

#### Gene Spatial & Umap
        @self.auth.app.route('/api/v1/genes/gmnames',methods=['POST'])
        @self.auth.login_required
        def _getGeneMotifNames():
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.get_GeneMotifNames(req)
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
        @self.auth.app.route('/api/v1/genes/gmnames/<token>',methods=['POST'])
        def _getGeneMotifNamesByToken(token):
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.get_GeneMotifNamesByToken(token, req)
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
        @self.auth.app.route('/api/v1/genes/get_spatial_data',methods=['POST'])
        @self.auth.login_required
        def _getSpatialData():
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.get_SpatialData(req)
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
        
        @self.auth.app.route('/api/v1/genes/get_spatial_data/<token>',methods=['POST'])
        def _getSpatialDataByToken(token):
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.get_SpatialDataByToken(token, req)
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
                res=self.auth.generateLink(req, u, g)
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
              
        @self.auth.app.route('/api/v1/genes/get_summation',methods=['POST'])
        @self.auth.login_required 
        def _getSummation():
            sc=200
            res=None
            req=request.get_json()
            filename = req['filename']
            rows = req['rows']
            try:
                res=self.get_Summation(filename, rows)
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

    def get_Summation(self, filename, rows):
      listOfData = []
      f = None
      endOfName = '.txt.gz'
      for i in rows:
        numberOfFile = 1
        findingRange = divmod(i, 1000)
        boundary = findingRange[0] * 1000
        if i <= 999: numberOfFile = 1
        if i >= boundary: numberOfFile = findingRange[0] + 1
        print('{}{}{}'.format(filename,numberOfFile,endOfName))
        currentRow = i
        if numberOfFile > 1: currentRow = abs(i - boundary)
        name = self.getFileObject(self.bucket_name, '{}{}{}'.format(filename,numberOfFile,endOfName))
        f = gzip.open(name, 'rt')
        amountOfTixels = f.readline()
        charValue = self.getCharValue(int(amountOfTixels), int(currentRow), len(amountOfTixels))
        f.seek(charValue)
        listOfData.append(f.readline().strip())
        f.close()
          
      f.close()
      return listOfData
        
    def get_GeneMotifNames(self,req):
      name = self.getFileObject(self.bucket_name, req['filename'])
      listOfElements = []
      whole_file = None
      if '.gz' not in name:
        whole_file = open(name, 'r')
        fileAsString = whole_file.read()
        listOfElements = fileAsString.split(',')
      else:
        whole_file = gzip.open(name, 'rt')
        fileAsString = whole_file.read()
        listOfElements = fileAsString.split(',')
      whole_file.close()
      return listOfElements[:-1]
    def get_GeneMotifNamesByToken(self, token, request):
      req = self.decodeLink(token, None, None)
      key = request['key']
      data = req['args'][key]
      return self.get_GeneMotifNames({'filename': data})
    def get_SpatialData(self, req):
      out = []
      name = self.getFileObject(self.bucket_name, req['filename'])
      if '.gz' not in req['filename']:
        with open(name,'r') as cf:
            csvreader = csv.reader(cf, delimiter=',')
            for r in csvreader:
                out.append(r)
      else:
        with gzip.open(name,'rt') as cf:
          csvreader = csv.reader(cf, delimiter=',')
          for r in csvreader:
              out.append(r)
      return out
    
    def get_SpatialDataByToken(self, token, request):
      req = self.decodeLink(token, None, None)
      key = request['key']
      data = req['args'][key]
      return self.get_SpatialData({'filename': data})
      
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

    def decodeLink(self,link,u,g):
        secret=self.auth.app.config['JWT_SECRET_KEY']
        return jwt.decode(link, secret,algorithms=['HS256'])
    def getCharValue(self, amount, row, lenOfTixels):
      data = amount * row  * 7
      return data + lenOfTixels 
    def checkFileExists(self,bucket_name,filename):
      try:
          object = self.aws_s3.head_object(Bucket=bucket_name, Key=filename)
          date = object['LastModified']
          size = object['ContentLength']
          return 200, True, date, size
      except:
          return 404, False, '', ''
    def getFileObject(self,bucket_name,filename):
        _,tf,date,size=self.checkFileExists(bucket_name,filename)
        temp_outpath=self.tempDirectory.joinpath(filename)
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            if temp_outpath.exists(): return str(temp_outpath)
            temp_outpath.parent.mkdir(parents=True, exist_ok=True)
            fp = open(temp_outpath, 'x')
            fp.close()
            modified_time = os.path.getmtime(temp_outpath)
            formatted = datetime.datetime.fromtimestamp(modified_time)
            if date.replace(tzinfo=None) != formatted and size > 0:
              f=open(temp_outpath,'wb+')
              self.aws_s3.download_fileobj(bucket_name,filename,f)
              f.close()

        return str(temp_outpath)

    def getFileObjectGzip(self,bucket_name,filename):
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_outpath=self.tempDirectory.joinpath(filename)
        if temp_outpath.exists(): 
          return str(temp_outpath), True
        temp_outpath.parent.mkdir(parents=True, exist_ok=True)
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            retr = self.aws_s3.get_object(Bucket=self.bucket_name, Key=filename)
            bytestream = io.BytesIO(retr['Body'].read())
            got_text = gzip.GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
            f = gzip.open(temp_outpath, 'wt')
            f.write(got_text)
            f.close()

        return got_text, False
              