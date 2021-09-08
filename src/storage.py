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

## aws
import boto3
from botocore.exceptions import ClientError

from . import utils 

class StorageAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.bucket_name=self.auth.app.config['bucket_name']
        self.aws_s3=boto3.client('s3')
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
            param_bucket=request.args.get('bucket',default=self.bucket_name,type=str)
            try:
                data_bytesio,_,size= self.getFileObject(param_bucket,param_filename)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Length']=size
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),404)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    

        @self.auth.app.route('/api/v1/storage/list',methods=['GET'])
        @self.auth.admin_required 
        def _getFileList():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('path',type=str)
            param_bucket=request.args.get('bucket',default=self.bucket_name,type=str)
            try:
                data= self.getFileList(param_bucket,param_filename)
                resp=Response(json.dumps(data,default=utils.datetime_handler),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),404)
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

                ## dev
                #return Response(None,status=200)
                ##
                payload={'uploaded_by':u.username}
                if 'meta' in request.values:
                    try:
                        payload.update(json.loads(request.values['meta']))
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
                payload={'uploaded_by':u.username}
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
        @self.auth.admin_required
        def _downloadFileByLink():
            sc=200
            res=None
            param_filename=request.args.get('filename',type=str)
            param_bucket=request.args.get('bucket',default=self.bucket_name,type=str)
            try:
                res= self.downloadFile_link(param_bucket,param_filename)
                res= utils.result_message(res)
            except Exception as e:
                res=utils.error_message(str(e))
            finally:
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp    


###### actual methods

    def uploadFile(self,bucket_name,fileobj,output_key,meta={}):
        try:

            temp_outpath=self.tempDirectory.joinpath(Path(fileobj.filename).name)
            _,tf=self.checkFileExists(bucket_name,output_key)
            if tf :
                #if not self.isFileExistInEntry(f.filename): self.insertEntry(meta)
                return utils.error_message("The file already exists",status_code=401)
            else:
                try:
                    ### save file in temporary disk
                    fileobj.save(str(temp_outpath))

                    ### move the file to s3
                    self.aws_s3.upload_file(str(temp_outpath),bucket_name,output_key)

                except Exception as e:
                    exc=traceback.format_exc()
                    self.auth.app.logger.exception(utils.log(exc))
                    return utils.error_message("Couldn't have finished to save the file and update database : {}, {}".format(str(e),exc),status_code=500)
            self.auth.app.logger.info("File saved {}".format(str(temp_outpath)))
            return utils.result_message(str(temp_outpath))
        except Exception as e:
            exc=traceback.format_exc()
            self.auth.app.logger.exception(utils.log(exc))
            return utils.error_message("Error during save the file: {} {} ".format(str(e),exc),status_code=500)

    def uploadFile_link(self,bucket_name,fileobj,output_key,meta={}):
        try:
            res=None
            _,tf=self.checkFileExists(bucket_name,output_key)
            if tf :
                #if not self.isFileExistInEntry(f.filename): self.insertEntry(meta)
                return utils.error_message("The file already exists",status_code=401)
            else:
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

    def downloadFile_link(self,bucket_name,filename):

        _,tf=self.checkFileExists(bucket_name,filename)
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            try:
                resp=self.aws_s3.generate_presigned_url('get_object',
                                                Params={'Key':filename,
                                                        'Bucket':bucket_name},
                                                ExpiresIn=3600)
            except Exception as e:
                exc=traceback.format_exc()
                self.auth.app.logger.exception(utils.log(exc))
                return utils.error_message("Couldn't have finished to get the link of the file: {}, {}".format(str(e),exc),status_code=500)
        self.auth.app.logger.info("File Link returned {}".format(str(resp)))
        return resp

    def getFileObject(self,bucket_name,filename):
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_outpath=self.tempDirectory.joinpath(Path(filename).name)
        ext=temp_outpath.suffix
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            f.seek(0)
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()

        return bytesIO, ext, size

    def getFileList(self,bucket_name,root_path):
        resp=self.aws_s3.list_objects_v2(Bucket=bucket_name,Prefix=root_path,StartAfter=root_path)
        return [f['Key'] for f in resp['Contents']]

    def checkFileExists(self,bucket_name,filename):
        return 404, False

###### utilities

    def getBucketName(self):
        return self.bucket_name













