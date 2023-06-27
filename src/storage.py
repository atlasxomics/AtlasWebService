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
# import miscellaneous modules
import os
import io
import traceback
import json
from pathlib import Path
import csv 
import cv2
import gzip
import glob
from flask_cors import CORS
## aws

from . import utils 

class StorageAPI:
    def __init__(self,app,**kwargs):
        self.app=app
        CORS(self.app)
        self.tempDirectory=Path(self.app.config['TEMP_DIRECTORY'])
        self.ldataDirectory=Path(self.app.config['LDATA_DIRECTORY'])
        self.initialize()
        self.initEndpoints()
    def initialize(self):
        ## make directory
        self.tempDirectory.mkdir(parents=True,exist_ok=True)

##### Endpoints

    def initEndpoints(self):
        @self.app.route('/api/v1/storage',methods=['GET'])
        def _getFileObject():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            try:
                data_bytesio,size,_= self.getFileObject(param_filename)
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
              
        @self.app.route('/api/v1/storage/image_as_jpg',methods=['GET'])
        def _getFileObjectAsJPG():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            rotation = request.args.get('rotation', type=int, default=0)
            flag=request.args.get('flag', False)
            try:
                data_bytesio,size,_= self.getFileObjectAsJPG(flag, filename= param_filename, rotation=rotation)
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

        @self.app.route('/api/v1/storage/png',methods=['GET'])
        def _getFileObjectAsPNG():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            flag=request.args.get('flag', False)
            try:
                data_bytesio,size,_= self.getImage(param_filename, flag)
                resp=Response(data_bytesio,status=200)
                resp.headers['Content-Type']='application/octet-stream'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    

        @self.app.route('/api/v1/storage/grayscale_image_jpg_cropping', methods=['GET'])
        def _getGrayImage():
            sc = 200
            res = None
            resp = None
            param_filename=request.args.get('filename',type=str)
            param_rotation=request.args.get('rotation', default=0, type=int)
            x1 = request.args.get('x1', type=int)
            x2 = request.args.get('x2', type=int)
            y1 = request.args.get('y1', type=int)
            y2 = request.args.get('y2', type=int)
            flag=request.args.get('flag', False)
            try:
                data_bytesio,size = self.get_gray_image_rotation_cropping_jpg(flag, param_filename, param_rotation, x1 = x1, x2 = x2, y1 = y1, y2 = y2)
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
        
        @self.app.route('/api/v1/storage/json',methods=['GET']) ### return json object from csv file
        def _getJsonFromFile():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            flag=request.args.get('flag', False)
            try:
                res = self.getJsonFromFile(param_filename, flag)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp  

        @self.app.route('/api/v1/storage/csv',methods=['GET']) ### return json object from csv file
        def _getCsvFileAsJson():
            sc=200
            res=None
            resp=None
            param_filename=request.args.get('filename',type=str)
            flag=request.args.get('flag', False)
            try:
                res = self.getCsvFileAsJson(param_filename,flag)
                resp=Response(json.dumps(res),status=200)
                resp.headers['Content-Type']='application/json'
            except Exception as e:
                exc=traceback.format_exc()
                res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
            finally:
                return resp    
  

        @self.app.route('/api/v1/storage/list',methods=['POST'])
        def _getFileList():
            sc=200
            res=None
            resp=None
            req = request.get_json()
            param_filename= req.get('path', "")
            param_filter=req.get('filter', None)
            only_files = req.get('only_files', False)
            flag=req.get('flag', False)
            try:
                data= self.getFileList(flag, param_filename, param_filter, only_files)
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

        @self.app.route('/api/v1/storage/sub_folders',methods=['POST'])
        def _getSubFolders():
            sc=200
            res=None
            resp=None
            req = request.get_json()
            param_prefix=req.get('prefix', "")
            flag=req.get('flag', False)
            try:
                data= self.get_subfolders(param_prefix, flag)
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
     
            
###### actual methods

    def getFileObject(self,filename,flag):
      if flag: temp_outpath=self.ldataDirectory.joinpath(filename)
      else: temp_outpath=self.tempDirectory.joinpath(filename)
      print(temp_outpath)
      tf = self.checkFileExists(temp_outpath)
      if not tf :
        return utils.error_message("The file doesn't exists",status_code=404)
      else:
        f=open(temp_outpath, 'rb')
        bytesIO=io.BytesIO(f.read())
        size=os.fstat(f.fileno()).st_size
        f.close()
      return bytesIO, size , temp_outpath

    def rotate_file_object(self, relative_path, degree, flag):
        rel_path = Path(relative_path)
        if flag: path = self.ldataDirectory.joinpath(rel_path)
        else: path = self.tempDirectory.joinpath(rel_path)
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

    def getFileObjectAsJPG(self,flag, filename, rotation):
        _,_,name = self.getFileObject(filename, flag)
        img=cv2.imread(name.__str__(),cv2.IMREAD_COLOR)
        if rotation != 0:
            img = self.rotate_image_no_cropping(img=img, degree=rotation)
        bytesIO = self.get_img_bytes(img)
        size = bytesIO.getbuffer().nbytes
        return bytesIO, size , name.__str__()
    
    def crop_image(self,img, x1, x2, y1, y2):
        return img[y1: y2, x1: x2]

    def get_gray_image_rotation_cropping_jpg(self, flag, filename, rotation, x1, x2, y1, y2):
        rel_path = Path(filename)
        if flag: path = self.ldataDirectory.joinpath(rel_path)
        else: path = self.tempDirectory.joinpath(rel_path)
        img=cv2.imread(path.__str__(),cv2.IMREAD_COLOR)
        gray_img = img[:, :, 0]
        if rotation != 0:
            gray_img = self.rotate_image_no_cropping(gray_img, rotation)
        cropped = self.crop_image(gray_img, x1, x2, y1, y2)
        bytesIO = self.get_img_bytes(cropped)
        size = bytesIO.getbuffer().nbytes
        return bytesIO, size

    def get_gray_image_rotation_jpg(self, filename, rotation, flag):
        rel_path = Path(filename)
        if flag: path = self.ldataDirectory.joinpath(rel_path)
        else: path = self.tempDirectory.joinpath(rel_path)
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

    def getImage(self,filename, flag):
        _,_,name = self.getFileObject(filename, flag)
        f=open(name,'rb+')
        bytesIO=io.BytesIO(f.read())
        size=os.fstat(f.fileno()).st_size
        f.close()
        return bytesIO, size , name.__str__()



    def getJsonFromFile(self,filename,flag):
      _,_,name=self.getFileObject(filename, flag)
      out = json.load(open(name,'rb'))
      return out

    def getCsvFileAsJson(self,filename,flag):
        _,_,name=self.getFileObject(filename,flag)
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

    def get_subfolders(self, prefix, flag):
      if flag: root_dir = self.ldataDirectory.joinpath(prefix)
      else: root_dir = self.tempDirectory.joinpath(prefix)
      res = []
      for path in glob.glob(f'{root_dir}/*/'):
        ind = [x for x, v in enumerate(path) if v == '/']
        res.append(path[ind[-2] + 1: ind[-1]])

      return res

    def getFileList(self,flag, root_path, fltr=None, only_files = False): #get all pages
      #alter this to be a lambda function that filters based on the filters and also whether the object is a file or a folder
      def checkList(value, list):
        #can exclude an option if it is only looking for files and finds a folder
        if only_files and value.is_dir():
          return False
        if fltr is not None:
          for i in list:
            #know an option is valid if after passing the first condtion, it matches a filter
            if (i.lower() in value.name.lower()): 
              return value.name
          #if filter is true but it doesnt match any filter, then it is not valid
          return False
        # if it doesn't have a filter and passed the only files condition, then it is valid
        return value.name
      
      if flag: root_dir = self.ldataDirectory.joinpath(root_path)
      else: root_dir = self.tempDirectory.joinpath(root_path)
      res=[]
      for path in os.scandir(root_dir):
        temp = path.name
        if fltr is not None or only_files:
          temp = checkList(path, fltr)
        if temp != False: res.append(temp)
      return res 

    def checkFileExists(self, filename):
      try:
        os.path.exists(filename)
        return True
      except:
        return False
    













