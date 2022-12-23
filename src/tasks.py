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
import jwt

from celery import Celery
from celery.result import AsyncResult

class TaskAPI:
    def __init__(self,auth,datastore,**kwargs):

        self.auth=auth
        self.datastore=datastore
        self.bucket_name=self.auth.app.config['S3_BUCKET_NAME']
        self.tempDirectory=Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.task_table=self.datastore.getTable(self.auth.app.config['DATA_TABLES']['task.request']['table_name'])
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

#### Worker status
        @self.auth.app.route('/api/v1/workers',methods=['GET'])
        @self.auth.admin_required 
        def _getWorkerStatus():
            sc=200
            res=None
            try:
                u,g=current_user
                res=self.getWorkers()
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

        @self.auth.app.route('/api/v1/workers/summary',methods=['GET'])
        @self.auth.admin_required 
        def _getWorkerSummary():
            sc=200
            res=None
            try:
                u,g=current_user
                res=self.getWorkerSummary(u,g)
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

        @self.auth.app.route('/api/v1/workers/query_tasks', methods=['POST'])
        @self.auth.admin_required
        def _query_tasks():
            sc = 200
            try:
                params = request.get_json()
                ids = params['ids']
                res = self.query_task(ids)
            except Exception as e:
                sc = 500
                print(e)
                res = "Failure"
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route('/api/v1/workers/worker_tasks', methods=['GET'])
        @self.auth.admin_required
        def _worker_tasks():
            sc = 200
            try:
                res = self.get_worker_tasks()
            except Exception as e:
                sc = 500
                print(e)
                res = "Failure"
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

#### Task posting
        @self.auth.app.route('/api/v1/task',methods=['POST'])
        @self.auth.login_required
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

        @self.auth.app.route('/api/v1/public_task',methods=['POST'])
        def _runPublicTask():
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.runPublicTask(req)
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

        @self.auth.app.route('/api/v1/task_sync',methods=['POST'])
        @self.auth.admin_required 
        def _runTaskSync():
            sc=200
            res=None
            req=request.get_json()
            try:
                u, g=current_user
                res=self.runTaskSync(req, u, g)
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
        @self.auth.login_required 
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
        username="Anonymous"
        if user is not None:
            username= user.username
        task_obj={
            "_id": task_id,
            "name":task_name,
            "args":task_args,
            "kwargs":task_kwargs,
            "queue": queue,
            "requested_by": username,
            "requested_at": utils.get_timestamp()
        } 
        task_obj.update(meta)
        self.task_table.insert_one(task_obj)
        return task_obj

    def decodeJWT(self,token):
        secret=self.auth.app.config['JWT_SECRET_KEY']
        output=jwt.decode(token, secret,algorithms=['HS256'])
        return output

    def runTask(self, req, user, group):
        kwargs = req.get('kwargs',{})
        username = user.username
        if group:
            group = group[0]
        kwargs['username'] = username
        kwargs['group'] = group
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], user, group, {})
        return task_object

    def runPublicTask(self, req):
        key = req['key']
        meta= self.decodeJWT(req['args'][0])
        req['args'] = [self.decodeJWT(req['args'][0])['args'][key]] + req['args'][1:]
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], None, None, meta)
        return task_object

    def runTaskSync(self, req, user, group):
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], user, {})
        r.wait()
        return r.get()

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

    def get_workers(self):
        workers = []
        res = self.celery.control.inspect()

    def get_worker_tasks(self):
        worker_tasks = {}
        res = self.celery.control.inspect()
        active = res.active()
        reserved = res.reserved()
        for worker_name in active.keys():
            worker_tasks[worker_name] = []
            active_lis = active[worker_name]
            scheduled_lis = reserved[worker_name]
            worker_tasks[worker_name] = active_lis + scheduled_lis
        return worker_tasks


    def query_task(self, task_list):
        res = self.celery.control.inspect().query_task(task_list)
        print(res)
        return res

    def getWorkers(self):
        res = {
            "report" : self.celery.control.inspect().report(),
            "stats" : self.celery.control.inspect().stats(),
            "active" : self.celery.control.inspect().active(),
            "registered" : self.celery.control.inspect().registered(),
            "active_queues": self.celery.control.inspect().active_queues()
        }
        return res;

    def getWorkerSummary(self, user, group):
        res = self.getWorkers()
        summary=[]
        for worker_name, task_list in res['registered'].items():
            for task_name in task_list:
                temp= {
                    'worker': worker_name,
                    'task' : task_name,
                }
                try:
                    temp['queues'] = list(map(lambda x: x['name'],res['active_queues'][worker_name]))
                    ## read worker params
                    payload = {
                        "task":  task_name.split('.')[0]+".task_list",
                        'queue': temp['queues'][0],
                        'args': [],
                        'kwargs': {}
                    }
                    task_params = self.runTaskSync(payload, user, group)
                    try:
                        temp['params'] = task_params['tasks'][task_name]
                    except:
                        temp['params'] = None
                except Exception as e:
                    # print(traceback.format_exc())
                    temp['queues'] = []
                try:
                    temp['requests'] = res['stats'][worker_name]['total'][task_name]
                except:
                    temp['requests'] = 0
                summary.append(temp)
        return summary




