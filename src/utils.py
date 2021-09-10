##################################################################################
### Module : utils.py
### Description : Utility functions 
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/04
### Copyrighted reserved by AtlasXomics
##################################################################################


import json
from flask import request 
from flask_jwt_extended import current_user

import hmac
import hashlib
import base64
import uuid
import os,traceback
import datetime

## Utilities like loggers 

def get_secret_hash(username,client_id,client_secret):
    msg = username + client_id
    dig = hmac.new(str(client_secret).encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2

## json serialize handlers
def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

## uuid and datetime
def get_uuid():
    return str(uuid.uuid4())

def get_timestamp():
    return datetime.datetime.now().isoformat()
## logger

def get_user_ip():
    headers_list = request.headers.getlist("X-Forwarded-For")
    user_ip = headers_list[0] if headers_list else request.remote_addr
    return user_ip

def log(msg=""):
    username="anonymous"
    try:
        u,g=current_user    
        username=u.username 
    except Exception as e:
        pass

    s="[{} {}] <{}@{}> MSG: {} ".format(request.method,request.path,username,get_user_ip(),msg)
    return s

## dict to attributes

def dict_to_attributes(dict_obj):
    attrs=[]
    for k,v in dict_obj.items():
        attrs.append({
                'Name' : k,
                'Value' : v
            })
    return attrs 

def attributes_to_dict(attr_list):
    return [{x['Name'] : x['Value']} for x in attr_list]


## error message
def result_message(msg,status_code=200):
    return {"msg":msg,"status_code":status_code}

def error_message(msg,status_code=404):
    return {"msg":msg,"status_code":status_code}

