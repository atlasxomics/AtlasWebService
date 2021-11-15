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
from . import utils 

from celery import Celery
from celery.result import AsyncResult

class TaskAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.task_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['task.request']['table_name'])
        self.storageApi=self.auth.app.config['SUBMODULES']['StorageAPI']
        self.broker="amqp://{}:{}@{}".format(self.auth.app.config['RABBITMQ_USERNAME'],
                                              self.auth.app.config['RABBITMQ_PASSWORD'],
                                              self.auth.app.config['RABBITMQ_HOST'])
        self.backend="redis://:{}@{}".format( self.auth.app.config['REDIS_PASSWORD'],
                                              self.auth.app.config['REDIS_HOST'])
        self.celery=Celery('tasks', backend=self.backend, broker=self.broker)
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):

#### Task posting
        @self.auth.app.route('/api/v1/task',methods=['POST'])
        @self.auth.admin_required 
        def _runTask():
            sc=200
            res=None
            req=request.get_json()
            try:
                u,g=current_user
                res=self.runTask(req, u, g)
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
#### Task status check and retrieve the result if out
        @self.auth.app.route('/api/v1/task/<task_id>',methods=['GET'])
        @self.auth.admin_required 
        def _getTaskStatus(task_id):
            sc=200
            res=None
            try:
                u,g=current_user
                res=self.getTask(task_id)
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

    def createTaskObject(self, task_id, task_name, task_args, task_kwargs, queue, user, group, meta={}):
        task_obj={
            "_id": task_id,
            "name":task_name,
            "args":task_args,
            "kwargs":task_kwargs,
            "queue": queue,
            "requested_by": user.username,
            "requested_at": utils.get_timestamp()
        } 
        task_obj.update(meta)
        self.task_table.insert_one(task_obj)
        return task_obj

    def runTask(self, req, user, group):
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], user, group, {})
        return task_object

    def getTask(self, task_id):
        task= AsyncResult(task_id, backend=self.celery.backend)
        try:
            task.ready()
            res={
                "task_id":task.id,
                "status":task.state
            }
            if task.state=="PROGRESS":
                res['progress'] = task.info.get('progress')
                res['position'] = task.info.get('position')
            idquery = self.task_table.find_one({"_id": task.id})
            if idquery is None:
                return utils.error_message("There is no such task id requested", 500)
            if task.state=="SUCCESS":
                result=task.get()
                res['result']=result
        except Exception as e:
            raise Exception({"message": "Task has been failed","detail": str(e)})
        return res






