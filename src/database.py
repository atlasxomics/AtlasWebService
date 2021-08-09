##################################################################################
### Module : database.py
### description : DynamoDB class
### Requirements : DynamoDB (boto3)
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/02
### Copyrighted reserved by AtlasXomics
##################################################################################

import boto3
from botocore.exceptions import ClientError
from warrant import Cognito
# from flask_restful import Resource, Api

import datetime
from functools import wraps
from enum import IntEnum
import os 
import json
import uuid
import traceback

from . import utils

class DynamoDB(object):
    def __init__(self,auth):
        self.auth=auth
        self.client=None
        self.initialize()

    def initialize(self):
        try:
            self.client=boto3.resource('dynamodb')
        except Exception as e:
            pass 
        #existing_tables = [t.name for t in self.client.tables.all()]

    def getDatabase(self):
        return self.client 

    def getTable(self,table_name):
        return self.client.Table(table_name)

    ### member functions

