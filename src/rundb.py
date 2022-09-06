from crypt import methods
import mysql.connector
from flask import request, Response , send_from_directory
from flask_jwt_extended import jwt_required,get_jwt_identity,current_user
from werkzeug.utils import secure_filename
import traceback
import json
from pathlib import Path
from . import utils
from requests.auth import HTTPBasicAuth

class MariaDB:
    def __init__(self, auth):
        self.auth = auth
        self.client = None
        self.host = self.auth.app.config["MYSQL_HOST"]
        self.port = self.auth.app.config["MYSQL_PORT"]
        self.username = self.auth.app.config["MYSQL_USERNAME"]
        self.password = self.auth.app.config["MYSQL_PASSWORD"]
        self.db = self.auth.app.config["MYSQL_DB"]
        self.initialize()
        self.initEndpoints()
    
    def initialize(self):
        try:
            self.client = mysql.connector.connect(user=self.username, password=self.password,  host=self.host, port=self.port, database = self.db)
            self.cursor = self.client.cursor()
            print(self.client)
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

    def initEndpoints(self):

        @self.auth.app.route('/api/v1/run_db/get_columns_runid', methods=['GET'])
        def _getColumns():
            print('wahoo')
            run_ids = json.loads(request.args.get('run_ids', default=[]))
            columns = json.loads(request.args.get('columns', default=[]))
            table = request.args.get('table', default="dbit_metadata", type=str)
            status_code = 200
            print(run_ids)
            print(columns)
            try:
                res = self.getColumns(run_ids, columns, table)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

    def getColumns(self, run_ids, columns, table):
        sql1 = "SELECT "
        if len(columns) > 0:
            for i in range(len(columns)):
                if i != len(columns) - 1:
                    add = columns[i] + ", "
                else:
                    add = columns[i]
                sql1 += add
        else:
            sql1 += "*"

        sql2 = " FROM {}".format(table)
        if len(run_ids) > 0:
            tup = tuple(run_ids)
            sql3 = " WHERE cntn_cf_runId in {};".format(tup)
        else:
            sql3 = ";"
        sql = sql1 + sql2 + sql3
        print(sql)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        print(result)
        return result





