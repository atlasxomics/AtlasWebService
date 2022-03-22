##################################################################################
### Module : auth.py
### Description : Authentication & Authorization module for Flask-JWT , AWS Cognito
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/04
### Copyrighted reserved by AtlasXomics
##################################################################################

from flask import Flask , request, Response, jsonify,redirect
from flask_jwt_extended import jwt_required,get_jwt_identity,JWTManager, create_access_token,current_user
from flask_cors import CORS

# from flask_restful import Resource, Api

from werkzeug.security import safe_str_cmp
from hashlib import sha256
import datetime
from functools import wraps
from enum import IntEnum
import os 
import json
import uuid
import traceback

from . import utils

## aws
import boto3
from botocore.exceptions import ClientError
from warrant import Cognito


class Auth(object):
    def __init__(self,app,**kwargs):
        
        self.tablename='users'
        self.app=app 
        # self.auth=boto3.client('cognito-idp')
        self.aws_params={
            "app_key": self.app.config['AWS_ACCESS_KEY_ID'],
            "secret_key": self.app.config['AWS_SECRET_ACCESS_KEY'],
            "region": self.app.config['AWS_DEFAULT_REGION']
        }
        self.cognito_params={
                'pool_id' : self.app.config['AWS_COGNITO_USER_POOL_ID'],
                'client_id' :self.app.config['AWS_COGNITO_USER_POOL_CLIENT_ID'],
                'client_secret' : self.app.config['AWS_COGNITO_USER_POOL_CLIENT_SECRET']
        }
        os.environ['AWS_ACCESS_KEY_ID']=self.app.config['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=self.app.config['AWS_SECRET_ACCESS_KEY']
        os.environ['AWS_DEFAULT_REGION']=self.app.config['AWS_DEFAULT_REGION']

        os.environ['AWS_COGNITO_USER_POOL_ID']=self.app.config['AWS_COGNITO_USER_POOL_ID']
        os.environ['AWS_COGNITO_USER_POOL_CLIENT_ID']=self.app.config['AWS_COGNITO_USER_POOL_CLIENT_ID']
        os.environ['AWS_COGNITO_USER_POOL_CLIENT_SECRET']=self.app.config['AWS_COGNITO_USER_POOL_CLIENT_SECRET']

        CORS(self.app)
        self.jwt = JWTManager(self.app)
        self.aws_cognito=boto3.client('cognito-idp')
        self.jwt_cognito_map={} #jwt token to aws token

        #registering id
        @self.jwt.user_identity_loader
        def user_identity_lookup(user):
            return user

        #registering user lookup callback
        @self.jwt.user_lookup_loader
        def user_lookup_callback(_jwt_header,jwt_data):
            identity=jwt_data["sub"]
            # obj=self.dbInstance.find_one(self.tablename,{"username":identity})
            username=identity 
            u=Cognito(self.cognito_params['pool_id'],
                       self.cognito_params['client_id'],
                       client_secret=self.cognito_params['client_secret'],
                       username=username)

            user=u.admin_get_user()
            g=self.aws_cognito.admin_list_groups_for_user(Username=username,UserPoolId=self.cognito_params['pool_id'])
            groups=[]
            try:
                groups=[x['GroupName'] for x in g['Groups']]
            except Exception as e :
                groups=[]
            if user is not None:
                return (user ,groups)
            else:
                return (None,[])


        ### create admin if not existing
        self.create_admin()

        ### Register auth APIs to flask app
        self.registerAuthUri() 

    def registerAuthUri(self):

        @self.app.route("/api/v1/app", methods=["GET"])
        def _check_app():
            version=self.app.config['APP_VERSION']
            return jsonify(version)


        @self.app.route("/api/v1/auth/login", methods=["POST"])
        def login():
            msg=None
            req=request.get_json()
            username = req['username']
            password = req['password']
            u , access_token=self.authenticate(username,password)
            if u is None:
                return jsonify({"msg": "Bad username or password"}), 401

            msg="{} is logged in".format(username)
            self.app.logger.info(utils.log(msg))
            return jsonify(access_token=access_token)

        @self.app.route('/api/v1/auth/refreshtoken',methods=['POST'])
        @jwt_required(refresh=False)
        def _refresh_token():
            identity = get_jwt_identity()
            access_token = create_access_token(identity=identity)
            return jsonify(access_token=access_token)

        @self.app.route('/api/v1/auth/api_token', methods=['GET'])
        @jwt_required()
        def _issue_api_token(): ## 1 year
            params_expiry_days = request.args.get('expiry_days',default=365,type=int)
            identity = get_jwt_identity()
            expires = datetime.timedelta(days=params_expiry_days)
            access_token = create_access_token(identity=identity, expires_delta=expires)
            return jsonify(access_token=access_token)    
                   
        @self.app.route('/api/v1/auth/user',methods=['GET']) # get user list
        @self.admin_required
        def _get_users():
            resp=None
            msg=None
            try:
                res=self.get_user_list()
                resp=Response(json.dumps(res,sort_keys=True,indent=4),200)
            except Exception as e:
                resp=Response(None,404)
            finally:
                self.app.logger.info(utils.log(msg))
                resp.headers['Content-Type']='application/json'
                return resp 



        @self.app.route("/api/v1/auth/user", methods=["POST"])  #register user
        @self.admin_required 
        def register_user():
            msg=None
            req=request.get_json()
            username=None
            password=None
            attributes=None
            resp=None

            try:
                username = req['username']
                password = req['password']
                attributes={
                    "name" : req['name'],
                    "email" : req['email']
                }
                res=self.register(username,password,attributes)
                resp=Response(json.dumps(res), 200)
                msg="{} is created".format(username)
                self.app.logger.info(utils.log(msg))
                
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to create the user {} : {}".format(username,str(e)),401)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/user/<username>',methods=['GET'])
        @self.admin_required
        def _get_user(username):
            resp=None
            msg=None 
            try:
                user=self.get_user(username)
                resp=Response(json.dumps(user,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to find the user {} : {}".format(username,str(e)),404)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 


        @self.app.route('/api/v1/auth/user/<username>',methods=['PUT'])
        @self.admin_required
        def _update_user_attrs(username):
            resp=None
            msg=None 
            req=request.get_json()
            attrs=req 
            try:
                user=self.update_user_attrs(username,attrs)
                resp=Response(json.dumps(user,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to find the user {} : {}".format(username,str(e)),404)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 



        @self.app.route('/api/v1/auth/user/<username>',methods=['DELETE'])  #delete user
        @self.admin_required
        def _delete_user(username):
            resp=None
            msg=None
            try:
                res=self.delete_user(username)
                resp=Response(json.dumps(res,sort_keys=True,indent=4),200)
            except Exception as e:
                resp=Response(None,404)
            finally:
                self.app.logger.info(utils.log(msg))
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route("/api/v1/auth/user")
        @self.app.route('/api/v1/auth/whoami',methods=["GET"])
        @jwt_required()
        def _whoami():
            msg=None
            out=self.get_myself()
            res=Response(json.dumps(out,default=utils.datetime_handler), 200)
            res.headers['Content-Type']='application/json'
            self.app.logger.info(utils.log(msg))
            return res


        @self.app.route('/api/v1/auth/confirm/<username>',methods=['PUT'])
        @self.admin_required
        def _confirm_user(username):
            resp=None
            msg=None 
            try:
                res=self.confirm_user(username)
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to confirm the signup of user {} : {}".format(username,str(e)),401)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/changepassword',methods=['PUT'])
        @jwt_required()
        def _change_password():
            resp=None
            msg=None 
            req=request.get_json() #{"old_password","new_password"}
            user,group=current_user
            username=user.username
            try:

                res=self.change_password(username,req['old_password'],req['new_password'])
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to change the password of user {} : {}".format(username,str(e)),401)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/resetpassword',methods=['PUT'])
        @self.admin_required
        def _reset_password():
            resp=None
            msg=None 
            req=request.get_json() #{"username":"string", new_password" :"password_to_reset"}
            err_message=None
            try:
                res=self.reset_password(req['username'],req['password'])
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to change the password of user {} : {} {}".format(req['username'],str(e),msg),401)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/group/<username>/<groupname>',methods=['PUT'])
        @self.admin_required
        def _add_user_to_group(username,groupname):
            resp=None
            msg=None 
            try:
                res=self.assign_group(username,groupname)
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to assign group '{}'' to user {} : {}".format(groupname,username,str(e)),401)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp             

        @self.app.route('/api/v1/auth/group',methods=['GET'])
        @self.admin_required
        def _list_groups():
            resp=None
            msg=None
            try:
                res=self.list_groups()
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to list groups : {}".format(str(e)),404)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/group',methods=['POST'])
        @self.admin_required
        def _create_groups():
            resp=None
            msg=None
            try:
                req=request.get_json()
                grpname = req['group_name']
                description = req['description']
                if not grpname: raise Exception("group_name is mandatory")
                if grpname.lower() in list(map(lambda x: x['GroupName'], self.list_groups()['Groups'])):
                    raise Exception('Group already exists')
                res=self.create_group(grpname, description)
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to create group : {}".format(str(e)),404)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

        @self.app.route('/api/v1/auth/group',methods=['DELETE'])
        @self.admin_required
        def _delete_groups():
            resp=None
            msg=None
            try:
                req=request.get_json()
                grpname = req['group_name']
                if not grpname: raise Exception("group_name is mandatory")
                if grpname.lower() not in list(map(lambda x: x['GroupName'], self.list_groups()['Groups'])):
                    raise Exception("Group doesn't exist")
                res=self.delete_group(grpname)
                resp=Response(json.dumps(res,default=utils.datetime_handler),200)
                self.app.logger.info(utils.log(msg))
            except Exception as e:
                msg=traceback.format_exc()
                err_message=utils.error_message("Failed to delete group : {}".format(str(e)),404)
                resp=Response(json.dumps(err_message),err_message['status_code'])
                self.app.logger.exception(utils.log(msg))
            finally:
                resp.headers['Content-Type']='application/json'
                return resp 

    ### JWT functions

    def authenticate(self,username, password): #### This is internal authentication function
        user = Cognito(self.cognito_params['pool_id'],
                       self.cognito_params['client_id'],
                       client_secret=self.cognito_params['client_secret'],
                       username=username)
        try:
            user.authenticate(password=password)
            access_token = create_access_token(identity=username)
            return user ,access_token
        except Exception as e:
            exc=traceback.format_exc()
            return None,None

    ### User creation and management API endpoints
    def create_admin(self):
        username='admin'
        password='Hello123!'
        attrs={'name':'atx-cloud-admin','email':'atxcloud@atlasxomics.com'}
        try:
            self.get_user(username)
        except:
            
            self.register(username,password,attrs)
            self.confirm_user(username)
            self.assign_group(username,'admin')

        print("Admin : {}, Password : {}".format(username,password))

    def register(self,username,password,attrs):
        client_id=self.cognito_params['client_id']
        client_secret=self.cognito_params['client_secret']
        user_attrs=[{'Name' : k,'Value': v } for k,v in attrs.items()]
        res=self.aws_cognito.sign_up(ClientId=client_id,
                    SecretHash=utils.get_secret_hash(username,client_id,client_secret),
                    Username=username,
                    Password=password,
                    UserAttributes=user_attrs
                   )
        return res

    def delete_user(self,username):
        res=self.aws_cognito.admin_delete_user(UserPoolId=self.cognito_params['pool_id'],
                                     Username=username)
        return res

    def confirm_user(self,username):
        res=self.aws_cognito.admin_confirm_sign_up(UserPoolId=self.cognito_params['pool_id'],
                                     Username=username)
        return res

    def change_password(self,username,old_password,new_password):
        res=None
        u,token=self.authenticate(username,old_password) 
        if u is not None:
            res=self.aws_cognito.admin_set_user_password(UserPoolId=self.cognito_params['pool_id'],
                                         Username=username,Password=new_password,Permanent=True)
        else:
            raise Exception("Old password is not correct")
        return res

    def reset_password(self,username,new_password):
        res=self.aws_cognito.admin_set_user_password(UserPoolId=self.cognito_params['pool_id'],
                                         Username=username,Password=new_password,Permanent=True)
        return res

    def create_group(self, groupname, description):
        res=self.aws_cognito.create_group(GroupName=groupname, Description=description,UserPoolId=self.cognito_params['pool_id'])
        return res 

    def delete_group(self, groupname):
        res=self.aws_cognito.delete_group(GroupName=groupname, UserPoolId=self.cognito_params['pool_id'])
        return res 

    def list_groups(self):
        res=self.aws_cognito.list_groups(UserPoolId=self.cognito_params['pool_id'])
        return res 
        
    def assign_group(self,username,group):
        res=self.aws_cognito.admin_add_user_to_group(UserPoolId=self.cognito_params['pool_id'],
                                                     Username=username,GroupName=group)
        return res 

    def get_user(self,username):
        user=self.aws_cognito.admin_get_user(UserPoolId=self.cognito_params['pool_id'],Username=username)
        g=self.aws_cognito.admin_list_groups_for_user(Username=username,UserPoolId=self.cognito_params['pool_id'])
        groups=[]
        try:
            groups=[x['GroupName'] for x in g['Groups']]
        except Exception as e :
            groups=[]
        user.update({'groups':groups})
        return user

    def update_user_attrs(self,username,attrs):
        attr_list=utils.dict_to_attributes(attrs)
        res=self.aws_cognito.admin_update_user_attributes(UserPoolId=self.cognito_params['pool_id'],
                                                          Username=username,
                                                          UserAttributes=attr_list)
        return res 

    def get_myself(self):
        u,g=current_user
        user=self.get_user(u.username)
        return user


    def get_user_list(self):
        u=Cognito(self.cognito_params['pool_id'],
                   self.cognito_params['client_id'])
        users=u.get_users()
        res=[]
        for u in users:
            res.append({
                    'username': u.username,
                    'attributes': u._data
                })
        return res 
        
    ########################### DECORATORS ###############################


    #### Authorization decorators by user level


    def admin_required(self,func):
        @jwt_required()
        @wraps(func)  # this is important when macro is expanded , this makes wrapper not redundant to prevent the error from defining same function twice
        def wrapper(*args,**kwargs):
            try:
                u,g = current_user 

                if 'admin' in g:
                    return func(*args,**kwargs)
                else:
                    res=utils.error_message("Admin required",401)
                    resp=Response(json.dumps(res),status=res['status_code'])
                    resp.headers['Content-Type']='application/json'
                    return resp
            except Exception as e:
                res=utils.error_message(str(e),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                return resp
        return wrapper

