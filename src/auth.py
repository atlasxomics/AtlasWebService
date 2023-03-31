##################################################################################
### Module : auth.py
### Description : Authentication & Authorization module for Flask-JWT , AWS Cognito
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/04
### Copyrighted reserved by AtlasXomics
##################################################################################

import email
from flask import Flask , request, Response, jsonify,redirect
from flask_jwt_extended import jwt_required,get_jwt_identity,JWTManager, create_access_token,current_user
from flask_cors import CORS

# from flask_restful import Resource, Api

from hashlib import sha256
import datetime
from functools import wraps
from enum import IntEnum
import os 
import json
import time
import uuid
import traceback
import string
import smtplib, ssl
import email.message
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from . import utils
import jwt
import sqlalchemy as db
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

        ### Create engine used to connect to relational DB
        self.create_engine()

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
            self.document_login(username)
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
            groups=None
            attributes=None
            resp=None
            try:
                username = req['username']
                password = req['password']
                groups = req['groups']
                attributes={
                    "name" : req['name'],
                    "email" : req['email']
                }
                # res=self.register(username,password,attributes)
                for i in groups:
                  self.assign_group(username, i)
                # resp=Response(json.dumps(res), 200)
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

        @self.app.route('/api/v1/auth/forgot_password_request', methods=['GET'])
        def _forgot_password_request():
            username = request.args.get('username', default="", type=str)
            sc = 200
            try:
                exists = self.check_user_exists(username=username)
                print('exists: {}'.format(exists))
                if exists:
                    state_dict = self.get_user_state(username=username)
                    state = state_dict['user_state']
                    email_verified = state_dict['email_verified']
                    if state.lower() == 'confirmed' and email_verified:
                        res = self.forgot_password(username)
                        res['state'] = 'Success'
                    elif state.lower() == 'confirmed' and not email_verified:
                        res = {'state': 'email_unconfirmed'}
                    elif state.lower() == 'unconfirmed':
                        res = {'state': 'needs_confirmation'}
                else:
                    res = {'state': 'user_NA' }
            except Exception as e:
                err = utils.error_message("Forgot password failure: {}".format(e))
                res = { 'state': 'Failure', 'msg': err['msg']}
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        
        @self.app.route('/api/v1/auth/forgot_password_confirmation', methods=["POST"])
        def _forgot_password_confirmation():
            vals = request.get_json()
            username = vals['username']
            new_pass = vals["password"]
            code = vals["code"]
            sc = 200
            try:
                self.confirm_forgot_password_code(username=username, code=code, new_password = new_pass)
                resp = Response("Success", 200)
            except Exception as e:
                msg = traceback.format_exc()
                msg = 'wrong_code'
                resp = Response(msg, sc)
            finally:
                return resp

        @self.app.route('/api/v1/auth/user_account_request', methods=['POST'])
        def _user_request_account():
            params = request.get_json()
            name = params['name']
            pi_name = params['pi_name']
            organization = params['organization']
            email = params['email']
            username = params['username']
            password = params['password']
            resp = None
            try:
                attrs = {
                    'email': email,
                    'name': name,
                    'custom:organization': organization,
                    'custom:piname': pi_name
                }
                exists = self.check_user_exists(username=username)
                if exists:
                    resp = Response('exists', 200)
                else:
                    registration = self.register(username, password, attrs)
                    self.notify_about_user_request(params)
                    self.add_user_to_relational_db(username)
                    resp = Response(json.dumps(registration), 200)
            except Exception as e:
                # traceback error and print it
                msg = traceback.format_exc()
                self.app.logger.exception
                print(msg)
                resp = Response('error', 500)
            finally:
                return resp

        @self.app.route('/api/v1/auth/list_accounts', methods=['GET'])
        def _list_accounts():
            sc = 200
            try:
                response = self.get_accounts()
                resp = Response(json.dumps(response), 200)
                resp.headers['Content-Type'] = 'application/json'
            except Exception as e:
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to retrieve users {}".format(msg))
                resp = Response(json.dumps(error_message), 500)
            finally:
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
                res=self.delete_user_cognito(username)
                self.delete_user_relational(username)
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


        @self.app.route('/api/v1/auth/confirm_user_via_email', methods=["GET"])
        def _confirm_user_via_email():
            resp = "None"
            username = request.args.get('username', default='', type=str)
            code = request.args.get('confirmation_code', default='', type=str)
            try:
                result = self.confirm_user_via_code(username, code)
                resp = Response('Success', 200)
            except Exception as e:
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to confirm user via code user: {} error: .".format(username, str(e)))
                print(error_message)
                resp = Response(json.dumps("Failed"), 200)
            return resp
        
        @self.app.route('/api/v1/auth/resend_confirmation_via_email', methods=["GET"])
        def _resend_confirmation_via_email():
            username = request.args.get('username', default="", type=str)
            resp = "None"
            try:
                res = self.resend_confirmation_email(username)
                resp = Response("Success", 200)
            except Exception as e:
                print('error')
                resp = Response("Failure", 200)
            finally:
                return resp


        @self.app.route('/api/v1/auth/confirm',methods=['PUT'])
        @self.admin_required
        def _confirm_user():
            resp=None
            msg=None 
            req = request.get_json()
            username = req['data']['user']
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
        
        @self.app.route('/api/v1/auth/confirm_user_email_admin', methods=['POST'])
        @self.admin_required
        def _confirm_user_email_admin():
            req = request.get_json()
            username = req['username']
            try:
                sc = 200
                res = self.confirm_user_email_via_admin(username)
                msg = "Success"
            except Exception as e:
                msg = utils.error_message("Failed to confirm email: {}".format(str(e)))
                sc = 500
                print(msg)
            finally:
                resp = Response(json.dumps(msg),sc)
                return resp

        @self.app.route('/api/v1/auth/modify_group_list', methods=['PUT'])
        @self.admin_required
        def _modify_groups_list():
            sc = 200
            try:
                data = request.get_json()
                groups_adding = data.get('groups_adding', [])
                groups_removing = data.get('groups_removing', [])
                username = data['username']
                if len(groups_adding) > 0:
                    for groupname in groups_adding:
                        self.assign_user_to_group(username=username, group=groupname)
                if len(groups_removing) > 0:
                    for groupname in groups_removing:
                        self.remove_user_from_group(username=username, group=groupname)
                resp = Response("Success", 200)
            except Exception as e:
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to assign {} to groups: {}".format('username', msg))
                print(error_message)
                sc = 500
                resp = Response(json.dumps(error_message), sc)
            finally:
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
                grpname = req['data']['group_name']
                description = req['data']['description']
                if not grpname: raise Exception("group_name is mandatory")
                if grpname.lower() in list(self.list_groups()):
                    raise Exception('Group already exists')
                res=self.create_group(grpname, description)
                self.add_group_to_relational_db(grpname)
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
                req = request.get_json()
                grpname = req["group_name"]
                if not grpname: raise Exception("group_name is mandatory")
                if grpname.lower() not in list(self.list_groups()):
                    raise Exception("Group doesn't exist")
                res=self.delete_group(grpname)
                self.delete_group_db(grpname)
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


        #depreacted
        @self.app.route('/api/v1/auth/user_request', methods=['POST'])
        @self.admin_required
        def _new_user_request():
            resp = None
            status_code = 200
            args = request.get_json()
            try:
                self.notify_about_user_request(args)
                message = "Success"
            except Exception as e:
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to send notification email: {}".format(str(e)), 404)
                status_code = error_message["status_code"]
                message = "Failure"
            finally:
                resp = Response(json.dumps(message), status_code)
                return resp
        
        @self.app.route('/api/v1/auth/inform_user_assignment', methods=['POST'])
        @self.admin_required
        def _inform_user_assignment():
            resp = None
            sc = 200
            args = request.get_json()
            username = args.get("username")
            email = args.get("email")
            group = args.get("group")
            name = args.get("name")
            try:
                self.email_user_assignment(email, name, group)
                resp = Response("Success", 200)
            except Exception as e:
                # traceback error messages
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to send email: {} {}".format(str(e), msg), 404)
                print(error_message)
                resp = Response(json.dumps(error_message), 200)
            finally:
                return resp
              
        @self.app.route('/api/v1/auth/log_into_public', methods=['GET'])
        def _log_into_public():
            print("Logging in to public")
            res = None
            status_code = 200
            try:
                params_expiry_days = request.args.get('expiry_days',default=365,type=int)
                expires = datetime.timedelta(days=params_expiry_days)
                access_token = create_access_token(identity='public', expires_delta=expires)
                res = {'token': access_token}
                message = "Success"
            except Exception as e:
                print(e)
                msg = traceback.format_exc()
                error_message = utils.error_message("Failed to send notification email: {}".format(str(e)), 404)
                status_code = error_message["status_code"]
                message = "Failure"
            finally:
                resp = Response(json.dumps(res), status_code)
                return resp

        @self.app.route('/api/v1/auth/sync_user_table', methods=['POST'])
        def _sync_user_table():
            print("Logging in to public")
            res = None
            status_code = 200
            try:
                res = self.sync_user_table()
                message = "Success"
            except Exception as e:
                print(e)
                msg = traceback.format_exc()
                res = utils.error_message("Failed: {} {}".format(str(e), msg), 404)
                print(res)
            finally:
                resp = Response(json.dumps(res), status_code)
                return resp

    ### JWT functions

    def get_connection(self):
        conn = self.engine.connect()
        return conn
    
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

    def create_engine(self):
        username = self.app.config["MYSQL_HOST"]
        host = self.app.config["MYSQL_HOST"]
        port = self.app.config["MYSQL_PORT"]
        username = self.app.config["MYSQL_USERNAME"]
        password = self.app.config["MYSQL_PASSWORD"]
        db_name = self.app.config["MYSQL_DB"]
        connection_string = "mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}".format(username=username, password=password, host=host, port=str(port), dbname=db_name)
        self.engine = db.create_engine(connection_string)

    def add_user_to_relational_db(self, username):
        # write sql to insert user into table user_table if not already there. Values are username, and group_id
        conn = self.get_connection()
        select_user_sql = "SELECT user_id FROM user_table WHERE username = %s"
        tup = (username,)
        user_id = conn.execute(select_user_sql, tup).fetchone()
        if not user_id:
            insert_user_sql = "INSERT INTO user_table (username) VALUES (%s)"
            tup = (username,)
            conn.execute(insert_user_sql, tup)
        else:
            raise Exception("User already exists in relational database")


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

    def document_login(self, username):
        conn = self.get_connection()
        user_id = self.get_user_id(username)
        if not user_id:
            raise Exception("User does not exist in relational database")
        login_time_epoch = int(time.time())
        sql = "INSERT INTO user_logins (user_id, login_time) VALUES (%s, %s)"
        conn.execute(sql, (user_id, login_time_epoch))

    def sync_user_table(self):
        conn = self.get_connection()
        users = self.aws_cognito.list_users(UserPoolId = self.cognito_params['pool_id'])
        user_list = users['Users']
        for user in user_list:
            username = user['Username']
            user_info = self.get_user(username)
            groups = user_info.get('groups', [])
            group_ids = []
            for group in groups:
                # check if groups exist in groups_table
                group_id = self.get_group_id(group)
                group_ids.append(group_id)
                
            user_id = self.get_user_id(username) 
            for group_id in group_ids:
                check_sql = "SELECT * FROM user_group_table WHERE user_id = %s AND group_id = %s"
                res = conn.execute(check_sql, (user_id, group_id)).fetchone()
                if not res:
                    sql_insert_user_group = "INSERT INTO user_group_table (user_id, group_id) VALUES (%s, %s)"
                    conn.execute(sql_insert_user_group, (user_id, group_id))
                
        conn.close()
        return "Success"

    def delete_user_cognito(self,username):
        res=self.aws_cognito.admin_delete_user(UserPoolId=self.cognito_params['pool_id'],
                                     Username=username)
        return res

    def delete_user_relational(self, username):
        conn = self.get_connection()
        sql = "DELETE FROM user_table WHERE username = %s"
        conn.execute(sql, (username,))
        conn.close()

    def disable_user(self, username):
        res = self.aws_cognito.admin_disable_user(
            UserPoolId = self.cognito_params['pool_id'],
            Username=username)
        return res

    def confirm_user(self,username):
        res=self.aws_cognito.admin_confirm_sign_up(UserPoolId=self.cognito_params['pool_id'],
                                     Username=username)
        return res

    def confirm_user_via_code(self, username, code):
        client_id = self.cognito_params['client_id']
        client_secret = self.cognito_params['client_secret']
        res = self.aws_cognito.confirm_sign_up(
            ClientId=client_id,
            SecretHash=utils.get_secret_hash(username, client_id, client_secret),
            Username = username,
            ConfirmationCode = code
        )
        return res
    
    def resend_confirmation_email(self, username):
        client_id = self.cognito_params['client_id']
        client_secret = self.cognito_params['client_secret']
        res = self.aws_cognito.resend_confirmation_code(
            ClientId = client_id,
            SecretHash=utils.get_secret_hash(username, client_id, client_secret),
            Username = username
        )
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

    def forgot_password(self, username):
        client_id = self.cognito_params["client_id"]
        client_secret = self.cognito_params["client_secret"]
        res = self.aws_cognito.forgot_password(
            ClientId = client_id,
            SecretHash = utils.get_secret_hash(username, client_id, client_secret),
            Username = username
        )
        return res
    def confirm_forgot_password_code(self, username, code, new_password):
        client_id = self.cognito_params['client_id']
        client_secret = self.cognito_params['client_secret']
        res = self.aws_cognito.confirm_forgot_password(
            ClientId = client_id,
            SecretHash = utils.get_secret_hash(username, client_id, client_secret),
            Username = username,
            Password = new_password,
            ConfirmationCode = code )
        return res

    def reset_password(self,username,new_password):
        res=self.aws_cognito.admin_set_user_password(UserPoolId=self.cognito_params['pool_id'],
                                         Username=username,Password=new_password,Permanent=True)
        return res

    def create_group(self, groupname, description):
        res=self.aws_cognito.create_group(GroupName=groupname, Description=description,UserPoolId=self.cognito_params['pool_id'])
        return res 

    def delete_group_db(self, group_name):
        sql = """DELETE FROM groups_table WHERE group_name = %s"""
        conn = self.get_connection()
        tup = (group_name, )
        conn.execute(sql, tup)


    def add_group_to_relational_db(self, group_name):
        conn = self.get_connection()
        sql_check = """SELECT * FROM groups_table WHERE group_name = %s"""
        present = conn.execute(sql_check, (group_name,)).fetchone()
        if not present:
            sql = """INSERT INTO groups_table (group_name) VALUES (%s)"""
            tup = (group_name, )
            res = conn.execute(sql, tup)
            group_id = res.lastrowid
            return group_id
        else:
            raise Exception("Group already exists")

    
    def delete_group(self, groupname):
        res=self.aws_cognito.delete_group(GroupName=groupname, UserPoolId=self.cognito_params['pool_id'])
        return res 

    def list_groups(self):
        res=self.aws_cognito.list_groups(UserPoolId=self.cognito_params['pool_id'])
        resp = [res["Groups"][i]["GroupName"].lower() for i in range(len(res["Groups"]))]
        return resp
        
    def assign_group(self,username,group):
        try:
          results = self.create_group(groupname=group, description='')
        except Exception as e :
          pass
        res=self.aws_cognito.admin_add_user_to_group(UserPoolId=self.cognito_params['pool_id'],
                                                     Username=username,GroupName=group)
        return res 
    
    def assign_group_sql(self, username, group):
        user_id = self.get_user_id(username)
        group_id = self.get_group_id(group)
        sql = """INSERT INTO user_group_table (user_id, group_id) VALUES (%s, %s)"""
        conn = self.get_connection()
        conn.execute(sql, (user_id, group_id))
    
    def assign_user_to_group(self, username, group):
        self.assign_group(username, group)
        self.assign_group_sql(username, group)
        return "Success"
    
    def remove_user_from_group(self, username, group):
        self.remove_user_from_group_cognito(username, group)
        self.remove_user_group_sql(username, group)
        return "Success"
    
    def remove_user_from_group_cognito(self, username, group):
        res = self.aws_cognito.admin_remove_user_from_group(
            UserPoolId = self.cognito_params['pool_id'],
            Username = username,
            GroupName = group,
        )
        return res
    
    def get_group_id(self, group_name):
        conn = self.get_connection()
        select_group_sql = "SELECT group_id FROM groups_table WHERE group_name = %s"
        tup = (group_name,)
        group_id = conn.execute(select_group_sql, tup).fetchone()
        if group_id:
            group_id = group_id[0]
        else:
            group_id = self.add_group_to_relational_db(group_name)
        return group_id 
    
    def get_user_id(self, username):
        sql = """SELECT user_id FROM user_table WHERE username = %s"""
        conn = self.get_connection()
        res = conn.execute(sql, (username)).fetchone()
        if res is None:
            sql = """INSERT INTO user_table (username) VALUES (%s)"""
            res = conn.execute(sql, (username))
            user_id = res.lastrowid
        else:
            user_id = res[0]
        return user_id
    
    def remove_user_group_sql(self, username, group):
        conn = self.get_connection()
        group_id = self.get_group_id(group)
        user_id = self.get_user_id(username)
        sql_delete_entry = """DELETE FROM user_group_table WHERE user_id = %s AND group_id = %s"""
        conn.execute(sql_delete_entry, (user_id, group_id))
        

    def get_user_state(self, username: string) -> string:
        user = self.get_user(username=username)
        return_dict = {'email_verified': False}
        for attr in user['UserAttributes']:
            name = attr['Name']
            if name == 'email_verified':
                val = attr['Value']
                if val.lower() == 'true':
                    res = True
                else:
                    res = False
                return_dict['email_verified'] = res
        return_dict['user_state'] = user['UserStatus']
        return return_dict

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

    def check_user_exists(self, username):
        try:
            user = self.aws_cognito.admin_get_user(UserPoolId=self.cognito_params['pool_id'],
                                            Username = username)
            res = True
        except Exception as e:
            res = False
        finally:
            return res

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
                    'attributes': u._data,
                })
        return res
    
    def get_accounts(self):
        id = 0
        users = self.aws_cognito.list_users(UserPoolId = self.cognito_params['pool_id'])
        users_dict = [] 
        for user in users['Users']:
            subdict = {}
            subdict['id'] = id
            username = user['Username']
            subdict['username'] = username
            subdict['status'] = user['UserStatus']
            groups=self.aws_cognito.admin_list_groups_for_user(Username=username,UserPoolId=self.cognito_params['pool_id'])
            subdict['groups'] = []
            for group in groups['Groups']:
                subdict['groups'].append(group['GroupName'])
            for attribute in user['Attributes']:
                name = attribute['Name']
                # value = attribute['Value']
                if name == 'name' or name == 'email' or name == 'custom:organization' or name == "custom:piname" or name == "email_verified":
                    inx = name.find(':')
                    name = name[inx + 1: ]
                    val = attribute.get('Value', '')
                    subdict[name] = val
            if 'piname' not in subdict.keys():
                subdict['piname'] = ''
            if 'organization' not in subdict.keys():
                subdict['organization'] = ''
            users_dict.append(subdict)
            id += 1
        return users_dict

    def confirm_user_email_via_admin(self, username):
        res = self.aws_cognito.admin_update_user_attributes(
            UserPoolId = self.cognito_params['pool_id'],
            Username = username,
            UserAttributes=[
                {
                    "Name": "email_verified",
                    "Value": "true"
                }
            ]
        )
        return res

    def notify_about_user_request(self, user_info_pl):
        name = user_info_pl.get("name", "")
        username = user_info_pl.get("username", "")
        recipient = user_info_pl.get("email", "")
        pi_name = user_info_pl.get("pi_name", "")
        organization = user_info_pl.get("organization", "")
        sender = self.app.config["EMAIL_SENDER"]
        password = self.app.config["EMAIL_LOGIN_CRED"]
        mail = email.message.Message()
        mail["From"] = sender
        mail["To"] = sender 
        mail["Subject"] = "New User Request"
        mail.set_payload(
            """
            Name: {}\n
            Username: {} \n
            PI Name: {} \n
            Organization: {}\n
            Email: {}\n
            """.format(name, username,pi_name, organization, recipient)
        )
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP("smtp.office365.com", port=587) as smtpObj:
                smtpObj.starttls(context=context)
                smtpObj.login(sender, password)
                smtpObj.sendmail(sender, sender, mail.as_string())
        except Exception as e:
            exc=traceback.format_exc()
            res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
        
    def email_user_assignment(self, receiving_email, name ,group):
        print("Sending email to {}".format(receiving_email))
        sender = self.app.config["EMAIL_SENDER"]
        password = self.app.config["EMAIL_LOGIN_CRED"]
        # print the current directory and contents
        path = os.getcwd()
        image_path = os.path.join(path, "images/company_logo.png")
        with open(image_path, "rb") as f:
            img = MIMEImage(f.read(), 'jpg')
        img.add_header('Content-ID', '<logo>')
        email_body = email_body = f"""
                            <html>
                            <head>
                                <style>
                                /* Add some style to your email */
                                body {{
                                    font-family: Arial, sans-serif;
                                    font-size: 14px;
                                }}
                                h1 {{
                                    font-size: 14px;
                                }}
                                p {{
                                    line-height: 1.5;
                                    margin: 0 0 10px 0;
                                }}
                                </style>
                            </head>
                            <body>
                                <p>Hello {name},</p>
                                <p>
                                You have been authorized to view data from the {group} lab.
                                </p>
                                <p>
                                To access these runs, please login at <a href="https://web.atlasxomics.com">AtlasXomics</a>.
                                </p>
                                <p>
                                Thank you for choosing AtlasXomics. If you have any questions or need assistance, please don't hesitate to contact us.
                                </p>
                                <p>
                                Best regards,<br>
                                The AtlasXomics Team
                                </p>
                                <p><br>
                                <img src="cid:logo" width="35%" height="35%" alt="AtlasXomics Logo">
                                </p>
                            </body>
                            </html>
                            """
        try:
            message = MIMEMultipart()
            message['From'] = sender
            message['To'] = receiving_email
            message['Subject'] = "AtlasXomics Group Assignment"
            message.attach(MIMEText(email_body, 'html'))
            message.attach(img)
            with smtplib.SMTP("smtp.office365.com", 587) as session:
            # session = smtplib.SMTP("smtp.gmail.com", 587)
                session.starttls()
                # session.login(sender, password)
                session.login(sender, password)
                text = message.as_string()
                session.sendmail(sender, receiving_email, text)
                session.quit()
        except smtplib.SMTPRecipientsRefused as e_rec:
            exc = traceback.format_exc()
            res = utils.error_message(f"Exception: {str(e)} {exc}", 500)
            print(res)
        except Exception as e:
            exc=traceback.format_exc()
            res=utils.error_message("Exception : {} {}".format(str(e),exc),500)
            print(res)
            
    def generateLink(self,req,u,g):
      secret=self.app.config['JWT_SECRET_KEY']
      encoded_payload = jwt.encode(req, secret, algorithm='HS256')
      return {'encoded': encoded_payload}
        
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

    def login_required(self,func):
        @jwt_required()
        @wraps(func)  # this is important when macro is expanded , this makes wrapper not redundant to prevent the error from defining same function twice
        def wrapper(*args,**kwargs):
            try:
                u,g = current_user 

                if u:
                    return func(*args,**kwargs)
                else:
                    res=utils.error_message("No Credentials",401)
                    resp=Response(json.dumps(res),status=res['status_code'])
                    resp.headers['Content-Type']='application/json'
                    return resp
            except Exception as e:
                res=utils.error_message(str(e),500)
                resp=Response(json.dumps(res),status=res['status_code'])
                resp.headers['Content-Type']='application/json'
                return resp
        return wrapper
