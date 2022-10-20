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
from scipy import ndimage
import re
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
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
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
        @self.auth.app.route('/api/v1/storage/data',methods=['GET'])
        @self.auth.login_required 
        def _getDataFromServer():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            try:
                res = self.getDataFromFile(param_filename)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
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
            # param_hflip=request.args.get('hflip', default=False, type=lambda v: v.lower() == 'true')
            # param_vflip=request.args.get('vflip', default=False, type=lambda v: v.lower() == 'true')
            param_rotation=request.args.get('rotation', default=0, type=int)
            orientation = {'rotation': param_rotation}
            try:
                data_bytesio,_,size,_= self.getFileObjectAsJPG(param_bucket, param_filename, orientation=orientation)
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
            try:
                res = self.getJsonFromFile(param_bucket,param_filename)
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
            try:
                res = self.getCsvFileAsJson(param_bucket,param_filename)
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

        @self.auth.app.route('/api/v1/storage/list',methods=['GET'])
        @self.auth.login_required 
        def _getFileList():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('path',type=str)
            param_bucket=request.args.get('bucket_name',default=self.bucket_name,type=str)
            param_filter=request.args.get('filter',default=None, type=str)
            try:
                data= self.getFileList(param_bucket,param_filename, param_filter)
                resp=Response(json.dumps(data,default=utils.datetime_handler),status=200)
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
###### actual methods

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

    def generateQCEntry(self,bucket_name,root_directory): ## from uploaded files, generate database entry for 'studies'
        object_list=self.getFileList(bucket_name,root_directory)
        start_index=len(root_directory.split('/'))
        k=root_directory.split('/')[-1].split('.')[0]
        root="/".join(root_directory.split('/')[:-1])
        object_list=list(map(lambda fp: fp.split('/')[start_index-1:],object_list))
        output={}
        files=list(map(lambda z: "/".join(z) ,filter(lambda y: y[0].split('.')[0]==k,object_list)))
        temp_obj={
            "_id": utils.get_uuid(),
            "id":k,
            "metadata":{},
            "files":{
                "bucket": bucket_name,
                "root": root,
                "images":{},
                "data":{},
                "meta":{},
                "other":{}
            }
        }
        for fn in files:
            category='other'
            if '.png' in fn.lower() or '.tiff' in fn.lower() or '.jpg' in fn.lower() or '.jpeg' in fn.lower():
                category='images'
            elif '.json' in fn.lower() or '.yml' in fn.lower():
                category='meta'
            elif '.csv' in fn.lower() or '.tsv' in fn.lower() or '.mtx' in fn.lower() or '.stat' in fn.lower():
                category='data'
            else:
                category='other'
            temp_obj['files'][category][fn.split('/')[-1].split('.')[0]]=fn
            ## load metadata using api
            if 'metadata.json' in fn.lower():
                meta_filename="{}/{}".format(root,fn)
                temp_filename=self.tempDirectory.joinpath("{}.json".format(utils.get_uuid()))
                print(meta_filename)
                _,_,_, temp_filename=self.getFileObject(bucket_name,meta_filename)
                temp_obj['metadata']=json.load(open(temp_filename,'r'))
        del temp_obj['files']['other']
        output=temp_obj
        # insert entry
        tablename=self.auth.app.config['DATA_TABLES']['studies.qc']['table_name']
        table=self.datastore.getTable(tablename)
        res=table.insert_many([output])
        return output 

    def deleteQCEntry(self,bucket_name, root_directory):
        output = {}
        object_list_paths=self.getFileList(bucket_name,root_directory)
        if len(object_list_paths) < 1 :
            return utils.result_message({"deleted_files" : 0 })
        start_index=len(root_directory.split('/'))
        k=root_directory.split('/')[-1].split('.')[0]
        root="/".join(root_directory.split('/')[:-1])
        object_list=list(map(lambda fp: fp.split('/')[start_index-1:],object_list_paths))
        output={}
        files=list(map(lambda z: "/".join(z) ,filter(lambda y: y[0].split('.')[0]==k,object_list)))
        qc_id = k

        # delete objects in s3
        deleted_count=0
        for s3obj_key in object_list_paths:
            r=self.deleteFile(bucket_name, s3obj_key)
            deleted_count += 1
        # delete entry
        tablename=self.auth.app.config['DATA_TABLES']['studies.qc']['table_name']
        table=self.datastore.getTable(tablename)
        fltr={ "id" : qc_id }
        res=table.delete_many(fltr)
        return utils.result_message({"deleted_files" : deleted_count})
      
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
        _,tf=self.checkFileExists(bucket_name,filename)
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
        _,tf=self.checkFileExists(bucket_name,filename)
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
    def getDataFromFile(self, filename):
      tf = os.path.exists(self.api_db.joinpath(filename))
      ext=Path(filename).suffix
      if not tf:
        return utils.error_message("The file doesn't exists",status_code=404)
      else:
        f=open(self.api_db.joinpath(filename),'r')
        bytesIO = 0
        size = 0
        out = json.load(open(self.api_db.joinpath(filename),'rb'))
        return bytesIO, ext, size , out
    def getFileObject(self,bucket_name,filename):
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_filename="{}_{}".format(utils.get_uuid(),Path(filename).name)
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            f.seek(0)
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()
            temp_outpath.unlink()
        return bytesIO, ext, size , temp_outpath.__str__()

    def getFileObjectAsJPG(self,bucket_name,filename, orientation):
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_filename="{}_{}".format(utils.get_uuid(),Path(filename).name)
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            f.close()
            img=cv2.imread(temp_outpath.__str__(),cv2.IMREAD_COLOR)
            if orientation['rotation'] != 0 :
                (h, w) = img.shape[:2]
                (cX, cY) = (w // 2, h // 2)
                # rotate our image by 45 degrees around the center of the image
                M = cv2.getRotationMatrix2D((cX, cY), orientation['rotation'], 1.0)
                rotated = cv2.warpAffine(img, M, (w, h))
                img = rotated
            temp_outpath=temp_outpath.parent.joinpath(temp_outpath.stem + ".jpg")
            cv2.imwrite(temp_outpath.__str__(), img, [cv2.IMWRITE_JPEG_QUALITY, 50])
            f=open(temp_outpath,'rb')
            f.seek(0)
            bytesIO=io.BytesIO(f.read())
            size=os.fstat(f.fileno()).st_size
            f.close()
            temp_outpath.unlink()
        return bytesIO, ext, size , temp_outpath.__str__()

    def getJsonFromFile(self, bucket_name, filename):
      try:
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_filename="{}_{}".format(utils.get_uuid(),Path(filename).name)
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            out=[]
            f.close()
            out = json.load(open(temp_outpath,'rb'))
            temp_outpath.unlink()
            return out
      except Exception as e:
        print(e)

    def getCsvFileAsJson(self,bucket_name,filename):
        _,tf=self.checkFileExists(bucket_name,filename)
        temp_filename="{}_{}".format(utils.get_uuid(),Path(filename).name)
        temp_outpath=self.tempDirectory.joinpath(temp_filename)
        ext=Path(filename).suffix
        tf=True
        if not tf :
            return utils.error_message("The file doesn't exists",status_code=404)
        else:
            f=open(temp_outpath,'wb+')
            self.aws_s3.download_fileobj(bucket_name,filename,f)
            out=[]
            f.close()
            with open(temp_outpath,'r') as cf:
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
        output_filename.unlink()
        return bytesIO, ext, size , output_filename.__str__()



    def getFileList(self,bucket_name,root_path, fltr=None): #get all pages
        paginator=self.aws_s3.get_paginator('list_objects')
        operation_parameters = {'Bucket': bucket_name,
                                'Prefix': root_path}
        page_iterator=paginator.paginate(**operation_parameters)
        res=[]
        for p in page_iterator:
            if 'Contents' in p:
                temp=[f['Key'] for f in p['Contents']]
                print(temp)
                if fltr is not None:
                    temp=list(filter(lambda x: fltr in x, temp))
                res+=temp
        return res 

    def checkFileExists(self,bucket_name,filename):
        try:
            self.aws_s3.head_object(Bucket=bucket_name, Key=filename)
            return 200, True
        except:
            return 404, False

###### utilities

    def getBucketName(self):
        return self.bucket_name













