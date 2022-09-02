import mysql.connector
from flask import request, Response , send_from_directory
from flask_jwt_extended import jwt_required,get_jwt_identity,current_user
from werkzeug.utils import secure_filename
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
import requests
from requests.auth import HTTPBasicAuth

class MariaDB(object):
    def __init__(self, auth):
        self.auth = auth
        self.client = None
        self.host = self.auth.app.config["MYSQL_HOST"]
        self.port = self.auth.app.config["MYSQL_PORT"]
        self.username = self.auth.app.config["MYSQL_USERNAME"]
        self.password = self.auth.app.config["MYSQL_PASSWORD"]
        self.db = self.auth.app.config["MYSQL_DB"]
        self.initialize()
    
    def initialize(self):
        try:
            self.client = mysql.connector.connect(user=self.username, password=self.password,  host=self.host, port=self.port)
            self.cursor = self.client.cursor()
            print(self.client)
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

