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
        self.aws_s3=boto3.resource('s3')
        self.aws_bucket=self.aws_s3.Bucket(self.bucket_name)
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        ## make directory
        self.tempDirectory.mkdir(parents=True,exist_ok=True)

##### Endpoints

    def initEndpoints(self):

        @self.auth.app.route('/api/v1/storage/upload',methods=['POST'])
        @self.auth.admin_required 
        def _uploadVideo():
            sc=200
            res=None
            try:
                res= self.uploadFile()
            except Exception as e:
                print(str(e))
                res=utils.error_message(str(e))
            finally:
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                self.auth.app.logger.info(utils.log(str(sc)))
                return resp  

        # @self.auth.app.route('/api/v1/storage/download/<filename>',methods=['GET'])
        # @self.auth.supervisor_required
        # def _downloadFile(filename):
        #     res=None
        #     try:
        #         destDir=self.getFileDirectory()
        #         path=destDir.joinpath(filename)
        #         if path.exists():
        #             return send_from_directory(str(destDir), filename, as_attachment=True)
        #         else:
        #             raise Exception("There is no such file")
        #     except Exception as e:
        #         print(str(e))
        #         sc, res=error_message(str(e))
        #         resp=Response(json.dumps(res),status=sc)
        #         resp.headers['Content-Type']='application/json'
        #         self.auth.app.logger.info(self.log(str(sc)))
        #         return resp  

        # @self.auth.app.route('/api/v1/videos/check/<name>',methods=['HEAD']) ## head doesn't have body, filename will be modified with prefix for checking
        # @self.auth.annotator_required
        # def _checkFile(name):
        #     sc=200
        #     res=None
        #     filename=name
        #     try:
        #         sc, res= self.checkFileExists(filename)
        #         res={"exists" : res}
        #     except Exception as e:
        #         #print(str(e))
        #         sc, res=error_message(str(e))
        #     finally:
        #         resp=Response(json.dumps(res),status=sc)
        #         resp.headers['Content-Type']='application/json'
        #         self.auth.app.logger.info(self.log(str(sc)))
        #         return resp  


###### actual methods

    def uploadFile(self):
        try:
            u=current_user
            destBucket=self.getBucketName()
            f=request.files['file']
            payload=json.loads(request.values['meta'])
            filename=f.filename
            temp_outpath=self.tempDirectory.joinpath(Path(filename).name)
            _,tf=self.checkFileExists(filename)
            if tf :
                #if not self.isFileExistInEntry(f.filename): self.insertEntry(meta)
                return utils.error_message("The file already exists",status_code=401)
            else:
                try:
                    ### save file in temporary disk
                    f.save(str(temp_outpath))

                    ### move the file to s3
                    self.aws_bucket.upload_file(str(temp_outpath),Path(temp_outpath).name)
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

    def checkFileExists(self,filename):
        return 404, False

###### utilities

    def getBucketName(self):
        return self.bucket_name













