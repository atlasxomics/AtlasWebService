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
            self.client = mysql.connector.connect(user=self.username, password=self.password,  host=self.host, port=self.port)
            self.cursor = self.client.cursor()
            print(self.client)
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

    def initEndpoints(self):

        @self.auth.app.route('/api/v1/run_db/get_columns_runid', methods=['GET'])
        @self.auth.login_required
        def _getColumns(self, methods=['GET']):
            print('wahoo')
            run_id = request.args.get('run_id', type=str)
            status_code = 200
            try:
                sql = """SELECT * FROM dbit_metadata
                        WHERE cntn_cf_runId = run_id
                    """
                res = self.cursor.execute(sql)
                print(res)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(data = json.dumps(res), status=status_code)
                return resp




