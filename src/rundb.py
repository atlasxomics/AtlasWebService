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
        
        @self.auth.app.route('/api/v1/run_db/reinitialize_db', methods=["GET"])
        def re_init():
            status_code = 200
            try:
                self.client = mysql.connector.connect(user=self.username, password=self.password,  host=self.host, port=self.port, database = self.db)
                self.cursor = self.client.cursor()
                res = "Success"
                print(self.client)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route('/api/v1/run_db/get_columns_runid', methods=['GET'])
        @self.auth.login_required
        def _getColumns():
            run_ids = json.loads(request.args.get('run_ids', default=[]))
            columns = json.loads(request.args.get('columns', default=[]))
            columns.append("cntn_cf_runId")
            table = request.args.get('table', default="dbit_metadata", type=str)
            status_code = 200
            try:
                res = self.getColumns(run_ids, columns, table)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_runs_collaborator", methods=["GET"])
        @self.auth.login_required
        def _getRunsCollaborator():
            collaborator = request.args.get('collaborator', default="", type=str)
            table = request.args.get('table', default="dbit_metadata", type=str)
            web_objs_only = request.args.get('web_objs', default=False, type=bool)
            status_code = 200
            try:
                res = self.getCollaboratorRuns(table, collaborator, web_objs_only)
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
        if len(run_ids) > 1:
            tup = tuple(run_ids)
            sql3 = " WHERE cntn_cf_runId in {};".format(tup)
        elif len(run_ids) == 1:
            sql3 = " WHERE cntn_cf_runId = '{}';".format(run_ids[0])
        else:
            sql3 = ";"
        sql = sql1 + sql2 + sql3
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        result_dict = self.list_to_dict(result, len(columns) - 1, columns)
        return result_dict

    def getCollaboratorRuns(self, table, collaborator, web_objs_only):
        sql1 = "SELECT * FROM " + str(table)
        sql2 = " WHERE cntn_cf_source = '{}'".format(collaborator)
        if web_objs_only:
            sql3 = " AND web_object_available = 1;"
        else:
            sql3 = ";"
        sql = sql1 + sql2 + sql3
        self.cursor.execute(sql)
        result_all = self.cursor.fetchall()
        cols = ["inx", "cntn_id_NGS", "cntn_cf_runId", "cntn_createdOn_NGS","cntn_cf_fk_tissueType", "cntn_cf_fk_organ", "cntn_cf_fk_species", "cntn_cf_experimentalCondition", "cntn_cf_sampleId", "cntn_cf_source", "cntn_cf_disease", "cntn_cf_tissueSlideExperimentalCondition", "web_object_available"]
        result_dict = self.list_of_dicts(result_all, cols)
        return result_dict 

    def list_to_dict(self, lis, key_inx, cols):
        final_dict = {}
        for item in lis:
            key = item[key_inx]
            final_dict[key] = {}
            for i in range(len(item)):
                if i != key_inx:
                    final_dict[key][cols[i]] = item[i]
        return final_dict

    def list_of_dicts(self, lis, cols):
        final_lis = []
        for item in lis:
            sub_dict = {}
            for i in range(len(item)):
                sub_dict[cols[i]] = item[i]
            final_lis.append(sub_dict)
        return final_lis




