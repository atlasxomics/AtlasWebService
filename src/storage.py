##################################################################################
### Module : storage.py
### Description : Storage API , AWS S3
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/04
### Copyrighted reserved by AtlasXomics
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
import csv 
import cv2
import jwt
from scipy import ndimage
import re
import gzip
import base64
from PIL import Image
## aws
import boto3
from botocore.exceptions import ClientError

from . import utils 

class StorageAPI:
    def __init__(self,auth,datastore,**kwargs):
        self.auth=auth
        self.datastore=datastore
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.api_db = Path(self.auth.app.config['API_DIRECTORY'])
        webpage_dir = self.auth.app.config.get('WEBPAGE_DIRECTORY',"")
        self.webpage_dir = Path(webpage_dir)
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.aws_s3 = boto3.client('s3')
        self.aws_s3_resource = boto3.resource('s3')
        self.initialize()
        self.initEndpoints()
    def initialize(self):
        ## make directory
        self.tempDirectory.mkdir(parents=True,exist_ok=True)

##### Endpoints

    def initEndpoints(self):
        @self.auth.app.route('/api/v1/storage',methods=['GET'])
        @self.auth.admin_required 
        def _getFileObject():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                data_bytesio,_,size,_= self.getFileObject(param_bucket,param_filename)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Length']=size
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    
              
        @self.auth.app.route('/api/v1/storage/image_as_jpg',methods=['GET'])
        @self.auth.login_required 
        def _getFileObjectAsJPG():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try_cache = request.args.get('use_cache', type=str, default='false')
            rotation = request.args.get('rotation', type=int, default=0)
            if try_cache == 'true':
                use_cache = True
            else:
                use_cache = False
            try:
                data_bytesio,_,size,_= self.getFileObjectAsJPG(bucket_name=param_bucket, filename= param_filename, try_cache= use_cache, rotation=rotation)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Length']=size
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                print(res)
            finally:
                return resp

        @self.auth.app.route('/api/v1/storage/png',methods=['GET'])
        def _getFileObjectAsPNG():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                data_bytesio,_,size,_= self.getImage(param_bucket, param_filename)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    

        @self.auth.app.route('/api/v1/storage/grayscale_image_jpg_cropping', methods=['GET'])
        @self.auth.login_required
        def _getGrayImage():
            sc = 200
            res = None
            resp = None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            param_rotation=request.args.get('rotation', default=0, type=int)
            x1 = request.args.get('x1', type=int)
            x2 = request.args.get('x2', type=int)
            y1 = request.args.get('y1', type=int)
            y2 = request.args.get('y2', type=int)
            try:
                data_bytesio,size = self.get_gray_image_rotation_cropping_jpg(param_filename, param_rotation, x1 = x1, x2 = x2, y1 = y1, y2 = y2)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Length']=size
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    
        
        @self.auth.app.route('/api/v1/storage/json',methods=['GET']) ### return json object from csv file
        @self.auth.login_required 
        def _getJsonFromFile():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            no_aws_yes_server = request.args.get('no_aws_yes_server', default = 'true')
            if no_aws_yes_server == 'false':
                no_aws_yes_server = False
            else:
                no_aws_yes_server = True
            try:
                res = self.getJsonFromFile(param_bucket,param_filename, no_aws_yes_server)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp  

        @self.auth.app.route('/api/v1/storage/csv',methods=['GET']) ### return json object from csv file
        @self.auth.login_required 
        def _getCsvFileAsJson():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            no_aws_yes_server = request.args.get('no_aws_yes_server', default='true', type=str)
            if no_aws_yes_server == 'false':
                no_aws_yes_server = False
            else:
                no_aws_yes_server = True
            try:
                res = self.getCsvFileAsJson(param_bucket,param_filename, no_aws_yes_server)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    

        @self.auth.app.route('/api/v1/storage/zip',methods=['GET'])
        @self.auth.admin_required
        def _getZippedDirectory():
            sc=200
            res=None
            resp=None
            param_root=request.args.get('root',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                data_bytesio,_,size,_= self.getFilesZipped(param_bucket,param_root)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Length']=size
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    

        @self.auth.app.route('/api/v1/storage/list',methods=['POST'])
        @self.auth.login_required 
        def _getFileList():
            sc=200
            res=None
            resp=None
            req = request.get_json()
            param_filename= req.get('path', "")
            param_bucket=req.get('bucket', self.bucket_name)
            param_filter=req.get('filter', None)
            param_delimiter = req.get('delimiter', None)
            only_files = req.get('only_files', False)
            try:
                data= self.getFileList(param_bucket,param_filename, param_filter, param_delimiter, only_files)
                resp=Response(json.dumps(data,default=utils.datetime_handler),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                print(res)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp   

        @self.auth.app.route('/api/v1/storage/sub_folders',methods=['POST'])
        @self.auth.login_required 
        def _getSubFolders():
            sc=200
            res=None
            resp=None
            req = request.get_json()
            param_bucket=req.get('bucket_name', self.bucket_name)
            param_prefix=req.get('prefix', "")
            try:
                data= self.get_subfolders(param_bucket, param_prefix)
                resp=Response(json.dumps(data,default=utils.datetime_handler),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                print(res)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp   

        @self.auth.app.route('/api/v1/storage/check_spatial_folder_exists', methods=['GET'])
        @self.auth.login_required
        def _check_spatial_folder_exists():
            sc=200
            res=None
            resp=None
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            param_folder=request.args.get('folder',type=str)
            param_root = request.args.get('root', type=str)
            try:
                res = self.check_spatial_folder_exists(param_bucket,param_folder, param_root)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp

        @self.auth.app.route('/api/v1/storage/upload',methods=['POST'])
        @self.auth.admin_required 
        def _uploadFile():
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

                if 'meta' in request.values:
                    try:
                        payload={}
                        payload.update(json.loads(request.values['meta']))
                        payload.update({'created_by':u.username})
                        payload.update({'created_at':utils.get_timestamp()})
                    except:
                        pass
                
                res= self.uploadFile(bucket_name,f,output_filename,meta=payload)
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

        @self.auth.app.route('/api/v1/storage/upload_link',methods=['POST'])
        @self.auth.admin_required 
        def _uploadFileByLink():
            sc=200
            res=None
            try:
                u=current_user
                bucket_name=request.values['bucket_name']
                output_filename=request.values['output_filename']
                f=request.files['file']
                payload={'created_by':u.username}
                if 'meta' in request.values:
                    try:
                        payload.update(json.loads(request.values['meta']))
                    except:
                        pass
                filename=f.filename
                res= self.uploadFile_link(bucket_name,f,output_filename,meta=payload)
            except Exception as e:
                sc=500
                res=utils.error_message(str(e))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  


        @self.auth.app.route('/api/v1/storage/download_link',methods=['GET'])
        @self.auth.login_required
        def _downloadFileByLink():
            sc=200
            res=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            param_expiry=request.args.get('expiry', default=3600, type=int)
            try:
                res= self.downloadFile_link(param_bucket,param_filename, param_expiry)
                res= utils.result_message(res)
            except Exception as e:
                res=utils.error_message(str(e))
            finally:
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp    

        @self.auth.app.route('/api/v1/storage/download_link_public',methods=['GET'])
        @self.auth.login_required
        def _downloadFileByLinkPublic():
            sc=200
            res=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                res= self.downloadFile_link_public(param_bucket,param_filename)
                res= utils.result_message(res)
            except Exception as e:
                res=utils.error_message(str(e))
            finally:
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp    

        @self.auth.app.route('/api/v1/storage/qc_entry',methods=['POST'])
        @self.auth.admin_required
        def _generate_qc_entry():
            sc=200
            res=None
            param_root=request.args.get('qc_dir',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                res= self.generateQCEntry(param_bucket,param_root)
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),500)
                sc=res['status_code']
                self.auth.app.logger.exception("{} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp    
  
        @self.auth.app.route('/api/v1/storage/qc_entry',methods=['DELETE'])
        @self.auth.admin_required
        def _delete_qc():
            sc=200
            res=None
            param_root=request.args.get('qc_dir',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            try:
                res= self.deleteQCEntry(param_bucket,param_root)
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),500)
                sc=res['status_code']
                self.auth.app.logger.exception("{} {}".format(str(e),exc))
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp     
        @self.auth.app.route('/api/v1/storage/decode_meta/<link>',methods=['GET'])
        @self.auth.login_required
        def _decodeMeta(link):
            sc=200
            res=None
            try:
                res=self.decodeInfo(link)
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
        @self.auth.app.route('/api/v1/storage/update_webimages',methods=['POST'])
        @self.auth.admin_required
        def _updateWebImages():
            sc=200
            res=None
            try:
                res=self.updateWebImages()
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
        @self.auth.app.route('/api/v1/storage/fetch_buckets',methods=['GET'])
        @self.auth.admin_required
        def _fetchBuckets():
            sc=200
            res=None
            try:
                res=self.grabAllBuckets()
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

        
        #create an endpoint post method that loads in the available json, pulls a aws file path from it and returns a presigned url to download that file
        @self.auth.app.route('/api/v1/storage/generate_presigned_urls',methods=['POST'])
        @self.auth.login_required
        def _generate_presigned_urls():
            sc=200
            pl = request.get_json()
            file_paths = pl.get('file_paths',{})
            res=None
            try:
                res=self.generatePresignedUrls(file_paths)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception
                print(res)
            finally:
                resp = Response(json.dumps(res),status=sc)
                return resp
            
            
        @self.auth.app.route('/api/v1/storage/move_files', methods=["POST"])
        @self.auth.login_required
        def _move_files():
            sc = 200
            pl = request.get_json()
            from_bucket = pl["from_bucket"]
            from_path = pl["from_path"]
            from_filter = pl["from_filter"]
            to_bucket = pl["to_bucket"]
            to_path = pl["to_path"]
            
            
            try:
                res = self.move_files(from_bucket, from_path, from_filter, to_bucket, to_path)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc), status_code = sc)
            finally:
                resp = Response(json.dumps(res), status=sc) 
                return resp
                
        @self.auth.app.route('/api/v1/storage/generate_presigned_url',methods=['POST'])
        @self.auth.login_required
        def _generate_presigned_url():
            sc=200
            pl = request.get_json()
            path = pl.get('path',None)
            bucket = pl.get("bucket", None)
            res=None
            try:
                res=self.generatePresignedUrl(path, bucket)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
                self.auth.app.logger.exception
                print(res)
            finally:
                resp = Response(json.dumps(res),status=sc)
                return resp
    

        @self.auth.app.route('/api/v1/storage/generate_frontpage',methods=['POST'])
        @self.auth.admin_required
        def _generateFrontPage():
            sc=200
            res=None
            try:
                data = request.get_json()
                url = data['url']
                runId = data['runId']
                res = self.screenShotImages(url, runId)
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
              
###### actual methods


    def generatePresignedUrls(self,file_paths):
        result = {}
        for id, info in file_paths.items():
            path = info.get('path', None)
            bucket = info.get('bucket', None)
            presigned_url = self.generatePresignedUrl(path, bucket)
            result[id] = presigned_url
        return result
    
    def generatePresignedUrl(self,path, bucket):
        if path == None:
            raise Exception('path not found')
        if not bucket:
            raise Exception("bucket not found")
        res = self.aws_s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                "Bucket": bucket,
                "Key": path,
            },
            ExpiresIn=3600
        )
        return res

    def move_files(self, from_bucket, from_path, from_filter, to_bucket, to_path):
        print(from_bucket, from_path, from_filter, to_bucket, to_path)
        files = self.getFileList(from_bucket, from_path, fltr = [from_filter], only_files = True)
        new_files = []
        for file in files:
            inx_substring = file.find(from_path)
            new_file = to_path + file[inx_substring + len(from_path):]
            new_files.append(new_file)
        
        
        for i in range(len(files)):
        # for i in range(10):
            from_file = files[i]
            to_file = new_files[i]
            copy_source = { 'Bucket': from_bucket,
                           'Key': from_file  
                           }
            print("from: ", from_bucket, " ", from_file )
            print("to: ", to_bucket, " ", to_file)
            self.aws_s3.copy(copy_source, to_bucket, to_file)
        
        for file in new_files:
            if from_filter not in file:
                print(file)
        # pag_config = {"MaxKeys": 1000, "Prefix": from_path, "Bucket": from_bucket}
        # paginator = self.aws_s3.get_paginator("list_objects")
        # result = paginator.paginate(**pag_config)
        
        
    
    #move all spatial folder images for the homescreen to be in an accessible folder
    def updateWebImages(self):
      allRuns = self.datastore.grab_runs_homepage_admin()
      for runs in allRuns:
        path = runs['results_folder_path']
        if path[0] == 's':
          path = 'S' + path[1:]
        runId = path.split(f'S3://{self.bucket_name}/data/')[1][:-1]
        awsPath = path.split(f'S3://{self.bucket_name}/')[1] + 'frontPage_{}.png'.format(runId)
        _, exists, _, _ = self.checkFileExists(self.bucket_name, awsPath)
        if not exists: break
        f=open('{path}/frontpage_images/frontPage_{run}.png'.format(path = self.webpage_dir, run = runId),'wb+')
        self.aws_s3.download_fileobj(self.bucket_name, awsPath, f)
        f.close()
      return {'outcome':'success'}
    def screenShotImages(self, url, run):
      base_string = url.replace("data:image/png;base64,", "")
      decoded_img = base64.b64decode(base_string)
      img = Image.open(io.BytesIO(decoded_img))
      img.thumbnail((200, 200))
      img.save('{}/frontpage_images/frontPage_{}.png'.format(self.webpage_dir, run))
    def grabAllBuckets(self):
      buckets = self.aws_s3.list_buckets()['Buckets']
      bucks = []
      for bucket in buckets:
        bucks.append(bucket['Name'])
      return bucks
    def decodeInfo(self, token):
      req = self.decodeLink(token, None, None)
      return req['meta']
    def uploadFile(self,bucket_name,fileobj,output_key,meta=None):
        try:
            temp_outpath=self.tempDirectory.joinpath("{}_{}".format(utils.get_uuid(),Path(fileobj.filename).name))
            #_,tf=self.checkFileExists(bucket_name,output_key)
            #if not self.isFileExistInEntry(f.filename): self.insertEntry(meta)
            try:
                ### save file in temporary disk
                fileobj.save(str(temp_outpath))
                ### move the file to s3
                self.aws_s3.upload_file(str(temp_outpath),bucket_name,output_key)
                temp_outpath.unlink()
                self.auth.app.logger.info("File saved {}".format(str(temp_outpath)))
                return utils.result_message(str(temp_outpath))                
            except Exception as e:
                exc=traceback.format_exc()
                self.auth.app.logger.exception(utils.log(exc))
                return utils.error_message("Couldn't have finished to save the file and update database : {}, {}".format(str(e),exc),status_code=500)
        except Exception as e:
            exc=traceback.format_exc()
            self.auth.app.logger.exception(utils.log(exc))
            return utils.error_message("Error during save the file: {} {} ".format(str(e),exc),status_code=500)
          
    def serverUploadFile(self,path,fileobj):
        try:
            ### save file in temporary disk
            fileobj.save(str(path))
            ### move the file to s3
            self.auth.app.logger.info("File saved {}".format(str(path)))
            return utils.result_message(str(path))                
        except Exception as e:
            exc=traceback.format_exc()
            self.auth.app.logger.exception(utils.log(exc))
            return utils.error_message("Couldn't finish saving the file : {}, {}".format(str(e),exc),status_code=500)

    def deleteFile(self,bucket_name, object_key):
        res=self.aws_s3.delete_object(Bucket=bucket_name, Key=object_key)
        return res 
      
    def uploadFile_link(self,bucket_name,fileobj,output_key,meta={}):
        try:
            res=None
            #_,tf=self.checkFileExists(bucket_name,output_key)
            try:
                res=self.aws_s3.generate_presigned_post(bucket_name,output_key,ExpiresIn=3600)
                self.auth.app.logger.info("File link generated {}".format(str(res)))
                return res
            except Exception as e:
                exc=traceback.format_exc()
                self.auth.app.logger.exception(utils.log(exc))
                return utils.error_message("Couldn't have finished to save the file and update database : {}, {}".format(str(e),exc),status_code=500)          
        except Exception as e:
            exc=traceback.format_exc()
            self.auth.app.logger.exception(utils.log(exc))
            return utils.error_message("Error during generating the upload link for the file: {} {} ".format(str(e),exc),status_code=500)

    def downloadFile_link(self,bucket_name,filename, expiry):
        _,tf,date,size=self.checkFileExists(bucket_name,filename)
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            try:
                resp=self.aws_s3.generate_presigned_url('get_object',
                                                Params={'Key':filename,
                                                        'Bucket':bucket_name},
                                                ExpiresIn=expiry)
            except Exception as e:
                exc=traceback.format_exc()
                self.auth.app.logger.exception(utils.log(exc))
                return utils.error_message("Couldn't have finished to get the link of the file: {}, {}".format(str(e),exc),status_code=500)
        self.auth.app.logger.info("File Link returned {}".format(str(resp)))
        return resp

    def downloadFile_link_public(self,bucket_name,filename):
        _,tf,date,size=self.checkFileExists(bucket_name,filename)
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            try:
                resp= "https://{}.s3.amazonaws.com/{}".format(bucket_name,filename)
            except Exception as e:
                exc=traceback.format_exc()
                self.auth.app.logger.exception(utils.log(exc))
                return utils.error_message("Couldn't have finished to get the link of the file: {}, {}".format(str(e),exc),status_code=500)
        self.auth.app.logger.info("File Link returned {}".format(str(resp)))
        return resp
    def getFileObject(self,bucket_name,filename, no_aws_yes_server = True):
      _,tf,date,size=self.checkFileExists(bucket_name,filename)
      temp_outpath=self.tempDirectory.joinpath(filename)
      ext=Path(filename).suffix
      if not tf :
        if temp_outpath.exists() and no_aws_yes_server:
          f=open(temp_outpath, 'rb')
          bytesIO=io.BytesIO(f.read())
          size=os.fstat(f.fileno()).st_size
          f.close() 
          return bytesIO, ext, size , temp_outpath
        else: return utils.error_message("The file doesn't exists",status_code=404)
      else:
          if not temp_outpath.exists():
            temp_outpath.parent.mkdir(parents=True, exist_ok=True)
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()
          else:
            modified_time = os.path.getmtime(temp_outpath)
            formatted = datetime.datetime.fromtimestamp(modified_time)
            if date.replace(tzinfo=None) > formatted and size > 0:
              f=open(temp_outpath,'wb+')
              self.aws_s3.download_fileobj(bucket_name,filename,f)
            else :
              f=open(temp_outpath, 'rb')
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()
          return bytesIO, ext, size , temp_outpath

    def rotate_file_object(self, relative_path, degree):
        rel_path = Path(relative_path)
        path = self.tempDirectory.joinpath(rel_path)
        img = cv2.imread(path.__str__(), cv2.IMREAD_COLOR)
        img = self.rotate_image_no_cropping(img, degree)
        bytesIO = self.get_img_bytes(img)
        size_bytes = bytesIO.getbuffer().nbytes
        return bytesIO, size_bytes
    
    def get_img_bytes(self, img):
        success, encoded = cv2.imencode('.jpg', img)
        bytes = encoded.tobytes()
        bytesIO = io.BytesIO(bytes)
        return bytesIO

    def getFileObjectAsJPG(self,bucket_name,filename, try_cache, rotation):
        _,tf,date,size=self.checkFileExists(bucket_name,filename)
        temp_filename="{}".format(Path(filename))
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        if try_cache and temp_outpath.exists():
            img = cv2.imread(temp_outpath.__str__(), cv2.IMREAD_COLOR)
        else:
            if temp_outpath.exists() == False: temp_outpath.parent.mkdir(parents=True, exist_ok=True)
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            f.close()
            img=cv2.imread(temp_outpath.__str__(),cv2.IMREAD_COLOR)
        if rotation != 0:
            img = self.rotate_image_no_cropping(img=img, degree=rotation)
        bytesIO = self.get_img_bytes(img)
        size = bytesIO.getbuffer().nbytes
        return bytesIO, ext, size , temp_outpath.__str__()
    
    def crop_image(self,img, x1, x2, y1, y2):
        return img[y1: y2, x1: x2]

    def get_gray_image_rotation_cropping_jpg(self, filename, rotation, x1, x2, y1, y2):
        rel_path = Path(filename)
        path = self.tempDirectory.joinpath(rel_path)
        img=cv2.imread(path.__str__(),cv2.IMREAD_COLOR)
        gray_img = img[:, :, 0]
        if rotation != 0:
            gray_img = self.rotate_image_no_cropping(gray_img, rotation)
        cropped = self.crop_image(gray_img, x1, x2, y1, y2)
        bytesIO = self.get_img_bytes(cropped)
        size = bytesIO.getbuffer().nbytes
        return bytesIO, size

    # def check_spatial_folder_exists(self, bucket_name, run_id, root_folder):
    #     try:
    #         self.aws_s3.head_object(Bucket=bucket_name, Key=)
    #         return True
    #     except ClientError:
    #         return False

    def get_gray_image_rotation_jpg(self, filename, rotation):
        rel_path = Path(filename)
        path = self.tempDirectory.joinpath(rel_path)
        img=cv2.imread(path.__str__(),cv2.IMREAD_COLOR)
        gray_img = img[:, :, 0]
        if rotation != 0:
            gray_img = self.rotate_image_no_cropping(gray_img, rotation)
        bytesIO = self.get_img_bytes(gray_img)
        size = bytesIO.getbuffer().nbytes
        return bytesIO, size

    def rotate_image_no_cropping(self, img, degree):
        (h, w) = img.shape[:2]
        (cX, cY) = (w // 2, h // 2)
        # rotate our image by 45 degrees around the center of the image
        M = cv2.getRotationMatrix2D((cX, cY), degree, 1.0)
        abs_cos = abs(M[0,0]) 
        abs_sin = abs(M[0,1])
        bound_w = int(h * abs_sin + w * abs_cos)
        bound_h = int(h * abs_cos + w * abs_sin)
        M[0, 2] += bound_w/2 - cX
        M[1, 2] += bound_h/2 - cY
        rotated = cv2.warpAffine(img, M, (bound_w, bound_h))
        return rotated

    def getImage(self,bucket_name,filename):
        _,tf,date,size=self.checkFileExists(bucket_name,filename)
        temp_filename="{}".format(Path(filename))
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            if temp_outpath.exists() == False: 
              temp_outpath.parent.mkdir(parents=True, exist_ok=True)
              f=open(temp_outpath,'wb+')
              self.aws_s3.download_fileobj(bucket_name,filename,f)
            else:
              f=open(temp_outpath,'rb+')
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()
        return bytesIO, ext, size , temp_outpath.__str__()



    def getJsonFromFile(self, bucket_name, filename, no_aws_yes_server):
      _,_,_,name=self.getFileObject(bucket_name,filename, no_aws_yes_server)
      out = json.load(open(name,'rb'))
      return out

    def getCsvFileAsJson(self,bucket_name,filename, no_aws_yes_server):
        _,_,_,name=self.getFileObject(bucket_name,filename, no_aws_yes_server)
        if '.gz' not in filename:
          out = []
          with open(name,'r') as cf:
            csvreader = csv.reader(cf, delimiter=',')
            for r in csvreader:
                out.append(r)
        else:
          out = []
          with gzip.open(name,'rt', encoding='utf-8') as cf:
            csvreader = csv.reader(cf, delimiter=',')
            for r in csvreader:
              out.append(r)
        return out

    def getFilesZipped(self,bucket_name, rootdir):
        filelist=self.getFileList(bucket_name,rootdir)
        temp_dir_name=utils.get_uuid()
        temp_rootdir=self.tempDirectory.joinpath(temp_dir_name)
        temp_rootdir.mkdir(parents=True,exist_ok=True)
        temp_filelist=[temp_rootdir.joinpath(Path(f)).__str__() for f in filelist]
        for idx,out_fn in enumerate(temp_filelist):
            Path(out_fn).parent.mkdir(parents=True,exist_ok=True)
            self.aws_s3.download_file(bucket_name, filelist[idx], out_fn)
            out_fn.close()
            os.remove(out_fn)
        output_filename=self.tempDirectory.joinpath("{}.zip".format(temp_dir_name))
        shutil.make_archive(self.tempDirectory.joinpath(temp_dir_name), 'zip', temp_rootdir.__str__())
        ext='zip'
        bytesIO=None
        size=0
        with open(output_filename,'rb') as f:
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
        return bytesIO, ext, size , output_filename.__str__()


    def get_subfolders(self, bucket_name, prefix):
        paginator_config = {"MaxKeys": 1000, "Prefix": prefix, "Bucket": bucket_name, "Delimiter": "/"}
        paginator = self.aws_s3.get_paginator("list_objects")
        result = paginator.paginate(**paginator_config)
        res = []
        for page in result:
            prefixes = page.get("CommonPrefixes", [])
            for prefix_obj in prefixes:
                full = prefix_obj["Prefix"]
                split = full.split(prefix)
                folder_name = split[1][:-1]
                res.append(folder_name)
        return res

    def getFileList(self,bucket_name,root_path, fltr=None, delimiter = None, only_files = False): #get all pages
      #alter this to be a lambda function that filters based on the filters and also whether the object is a file or a folder
      def checkList(value, list):
        #can exclude an option if it is only looking for files and finds a folder
        if only_files and value.endswith('/'):
          return False
        if fltr is not None:
          for i in list:
            #know an option is valid if after passing the first condtion, it matches a filter
            if (i.lower() in value.lower()): 
              return True
          #if filter is true but it doesnt match any filter, then it is not valid
          return False
        # if it doesn't have a filter and passed the only files condition, then it is valid
        return True
      
      if not bucket_name: bucket_name = self.bucket_name
          
      paginator=self.aws_s3.get_paginator('list_objects')
      operation_parameters = {'Bucket': bucket_name,
                              'Prefix': root_path
                              }
      if delimiter:
        operation_parameters['Delimiter'] = delimiter
      page_iterator=paginator.paginate(**operation_parameters)
      res=[]
      for p in page_iterator:
          if 'Contents' in p:
              temp=[f['Key'] for f in p['Contents']]
              if fltr is not None or only_files:
                temp=list(filter(lambda x: checkList(x, fltr), temp))
              res+=temp
      return res 

    def checkFileExists(self,bucket_name,filename):
      try:
          object = self.aws_s3.head_object(Bucket=bucket_name, Key=filename)
          date = object['LastModified']
          size = object['ContentLength']
          return 200, True, date, size
      except:
          return 404, False, '', ''
    
###### utilities

    def getBucketName(self):
        return self.bucket_name
    def decodeLink(self,link,u,g):
      secret=self.auth.app.config['JWT_SECRET_KEY']
      return jwt.decode(link, secret,algorithms=['HS256'])













