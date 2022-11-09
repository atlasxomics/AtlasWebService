from flask import request, Response , send_from_directory
from flask_jwt_extended import jwt_required,get_jwt_identity,current_user
from werkzeug.utils import secure_filename
import traceback
import json
from pathlib import Path
from . import utils
import requests
import pandas as pd
import sqlalchemy as db
from requests.auth import HTTPBasicAuth
import datetime
import boto3
import csv
import openpyxl

class MariaDB:
    def __init__(self, auth):
        self.auth = auth
        self.client = None
        self.host = self.auth.app.config["MYSQL_HOST"]
        self.port = self.auth.app.config["MYSQL_PORT"]
        self.username = self.auth.app.config["MYSQL_USERNAME"]
        self.password = self.auth.app.config["MYSQL_PASSWORD"]
        self.db = self.auth.app.config["MYSQL_DB"]
        self.tempDirectory = Path(self.auth.app.config['TEMP_DIRECTORY'])
        self.api_db = Path(self.auth.app.config['API_DIRECTORY'])
        self.initialize()
        self.initEndpoints()
        self.path_db = Path(self.auth.app.config["DBPOPULATION_DIRECTORY"])
        self.bucket_name = self.auth.app.config['S3_BUCKET_NAME']
        self.aws_s3 = boto3.client('s3')


    def initialize(self):
        try:
            connection_string = "mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}".format(username=self.username, password=self.password, host=self.host, port=str(self.port), dbname=self.db)
            self.engine = db.create_engine(connection_string)
            self.connection = self.engine.connect()
            print(self.connection)
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

    def initEndpoints(self):
        @self.auth.app.route('/api/v1/run_db/reinitialize_db', methods=["GET"])
        def re_init():
            status_code = 200
            try:
                self.connection.close()
                connection_string = "mysql+pymysql://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.db
                self.engine = db.create_engine(connection_string)
                self.connection = self.engine.connect()
                res = "Success"
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route('/api/v1/run_db/get_runs', methods=['GET'])
        @self.auth.login_required
        def _getRuns():
            table = request.args.get('table', default="all_run_data", type=str)
            status_code = 200
            try:
                res = self.getRuns(table, '')
            except Exception as e:
                print(e)
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route('/api/v1/run_db/get_group_runs', methods=['GET'])
        @self.auth.login_required
        def _getGroupRuns():
            table = request.args.get('table', default="all_run_data", type=str)
            group = request.args.get('group', default="", type=str)
            status_code = 200
            try:
                res = self.getRuns(table, group)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp
        
        @self.auth.app.route('/api/v1/run_db/modify_row', methods=['POST'])
        @self.auth.login_required
        def _modify_row():
            sc = 200
            try:
                # self.write_paths()
                # self.make_public()
                # self.set_groups()
                # self.set_groups_from_source()
                # self.add_descriptions()
                params = request.get_json()
                table = params["table"]
                print(params)
                args = params["changes"]
                on_var = params["on_var"]
                on_var_value = params["on_var_value"]
                self.edit_row(table, args, on_var, on_var_value)
                res = "Success"
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route('/api/v1/run_db/create_row', methods=["POST"])
        @self.auth.login_required
        def _create_row():
            sc = 200
            try:
                params = request.get_json()
                table_name = params["table_name"]
                values_dict = params["values_dict"]
                res = self.write_row(table_name, values_dict)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route('/api/v1/run_db/update_public', methods=['POST'])
        @self.auth.login_required
        def _updatePublicDB():
            status_code = 200
            try:
                res = self.createPublicTable()
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        # @self.auth.app.route('/api/v1/run_db/get_columns', methods=['GET'])
        # @self.auth.login_required
        # def _getColumns():
        #     ids = json.loads(request.args.get('ids', default=[]))
        #     columns = json.loads(request.args.get('columns', default=[]))
        #     on_var = request.args.get("match_on", default="", type=str)
        #     columns.extend(["run_id", "ngs_id"])
        #     table = request.args.get('table', default="dbit_metadata", type=str)

        @self.auth.app.route('/api/v1/run_db/get_columns_row', methods=['GET'])
        @self.auth.login_required
        def _getColumns():
            args = json.loads(request.args.get('0', default={}))
            ids = args.get('ids', [])
            columns = args.get('columns', [])
            columns.extend(["run_id", "ngs_id"])
            on_var = args.get('match_on', 'ngs_id')
            table = args.get('table', "dbit_metadata")
            status_code = 200
            try:
                res = self.getColumns(ids, columns, on_var, table)
            except Exception as e:
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route('/api/v1/run_db/get_ngs_ids', methods = ['GET'])
        @self.auth.login_required
        def _getNGSIds():
            try:
                ids = self.get_ngs_ids()
                resp = Response(json.dumps(ids), 200)
            except Exception as e:
                resp = Response('Unable to get run ids', 500)
            finally:
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


        @self.auth.app.route("/api/v1/run_db/repopulate_database2", methods = ["POST"])
        @self.auth.admin_required
        def _populatedb_2():
            status_code = 200
            print(" ###### REPOPULATING DB############# ")
            try:
                # (df_results, df_results_mixed) = self.pull_table("Result")
                # (df_experiment_run_step, experiment_run_step_mixed) = self.pull_table("ExperimentRunStep")
                (df_content, df_content_mixed) = self.pull_table("Content")
                # df_content.to_csv("content.csv")
                # df_content_mixed.to_csv("content_mixed.csv")
                # df_content = pd.read_csv("content.csv")
                # df_content_mixed = pd.read_csv("content_mixed.csv")
                # # # taking all fields needed for entire db scheme from the tissue slide content
                tissue_slides = self.grab_tissue_information(df_content, df_content_mixed)
                # #filtering the tissue_slides content into just being what is needed in the tissue table in the sql db
                tissue_slides_sql_table = self.get_tissue_slides_sql_table(tissue_slides.copy())

                tissue_slides_sql_table.drop(columns = ["tissue_id"], inplace=True, axis = 1)
                self.write_df(tissue_slides_sql_table, "tissue_slides")

                tissue_slide_inx = self.get_proper_index("tissue_id", "tissue_slides")
                # tissue_slide_inx = range(363)
                tissue_slides['tissue_id'] = tissue_slide_inx

                self.populate_antibody_table()
                antibody_dict = self.get_epitope_to_id_dict()

                # #using the tissue_slides/antibody df as well as the content table to create the run_metadata table
                run_metadata = self.create_run_metadata_table(df_content.copy(), tissue_slides, antibody_dict)
                print(run_metadata.shape)
                # run_metadata.to_csv("run_metadata.csv")

                # #creating the atlas runs df
                # atlas_runs_table = self.create_atlas_runs_sql_table(run_metadata)
                # atlas_runs_table.to_csv("atlas_runs_table.csv")
                # self.write_df(atlas_runs_table, "atlas_runs")

                run_metadata.drop(columns=["run_id", "results_id"], axis=1, inplace=True)
                # run_metadata.to_csv("run_metadata_sql.csv", index=False)
                self.write_df(run_metadata, "results_metadata")
                # atlas_runs_table.to_csv("atlas_runs_sql.csv")

                self.createPublicTable(antibody_dict)
                message = "Success"
            except Exception as e:
                print(e)
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                print(res)
                message = res
            finally:
                resp = Response(message, status=status_code)
                return resp


        @self.auth.app.route("/api/v1/run_db/create_database_tables", methods=["POST"])
        @self.auth.admin_required
        def _create_tables():
            sc = 200
            try:
                self.create_tables()
            except Exception as e:
                print("error!")
                print(e)
                sc = 500
            finally:
                return "foo"


        @self.auth.app.route("/api/v1/run_db/repopulate_database", methods=["GET"])
        @self.auth.login_required
        def _populatedb():
            status_code = 200
            try:
                print("##### REPOPULATING #####")
                (df_results, df_results_mixed) = self.pull_table("Result")
                (df_experiment_run_step, experiment_run_step_mixed) = self.pull_table("ExperimentRunStep")
                (df_content, df_content_mixed) = self.pull_table("Content")
                # # df_content.to_csv("content.csv")
                # # df_content_mixed.to_csv("content_mixed.csv")
                # # df_results.to_csv("Result.csv")
                # # df_results_mixed.to_csv("Results_mixed.csv")
                # # df_experiment_run_step.to_csv("ExperimentRunStep.csv")
                # # experiment_run_step_mixed.to_csv("ExperimentRunStepMixed.csv")
                # # print("tables pulled")
                # # df_content = pd.read_csv("Content.csv")
                # # df_content_mixed = pd.read_csv("content_mixed.csv")
                # # df_results = pd.read_csv("Result.csv")
                # # df_experiment_run_step = pd.read_csv("ExperimentRunStep.csv")

                df_tissue_meta = self.create_meta_table(df_content, df_content_mixed)
                df_bfx_results = self.create_bfx_table(df_content, df_results)
                df_flow_results = self.create_flow_table(df_content, df_results, df_experiment_run_step)

                self.write_df(df_tissue_meta, "dbit_metadata")
                self.write_df(df_bfx_results, "dbit_bfx_results")
                self.write_df(df_flow_results, "dbit_flow_results")
                status = "Success"
            except Exception as e:
                print(e)
                status_code = 500
                status = "Failure"
            finally:
                self.write_update(status)
                resp = Response(status, status=status_code)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_last_update", methods=["GET"])
        @self.auth.admin_required
        def _get_repopulation_date():
            sc = 200
            try:
                row = self.get_latest_date()
                dic = {
                    'date': str(row[1]),
                    'status': row[2]
                }
                resp = Response(json.dumps(dic), status=sc)
                resp.headers['Content-Type'] = 'application/json'
            except Exception as e:
                exc = traceback.format_exc()
                res = utils.error_message("Exception: {} {}".format(str(e), exc))
                sc = 500
                resp = Response(json.dumps(res), status=sc)
                resp.headers['Content-Type'] = 'application/json'
            finally:
                return resp

    def get_latest_date(self):
        sql = """ SELECT * FROM dbit_data_repopulations
                WHERE inx = (SELECT MAX(inx) FROM dbit_data_repopulations);
        """
        result = self.connection.execute(sql)
        row = result.fetchone()
        return row

    def get_proper_index(self, column_name, table_name):
        sql = f"SELECT  {column_name} FROM {table_name};"
        tuple_list = self.connection.execute(sql)
        index_list = [x[0] for x in tuple_list.fetchall()]
        return index_list
    
    def edit_row(self, table_name, changes_dict, on_var, on_var_value):
        update = f"UPDATE {table_name}"
        set_sql = " SET "
        for key, val in changes_dict.items():
            if isinstance(val, str):
                val = f"'{val}'"
            set_sql += f"{key} = {val}, "
        set_sql = set_sql[:len(set_sql) - 2]
        where = f" WHERE {on_var} = {on_var_value};"
        sql = update + set_sql + where
        print(sql)
        res = self.connection.execute(sql)

    def write_row(self, table_name, values_dict):
        INSERT = f"INSERT INTO {table_name} ("
        VALUES = ") VALUES ("
        for key, val in values_dict.items():
            if isinstance(val, str):
                val = f"'{val}'"
            INSERT += f"{key}, "
            VALUES += f"{val}, "

        INSERT = INSERT[ :len(INSERT) - 2]
        VALUES = VALUES[ :len(VALUES) - 2]
        sql = INSERT + VALUES + ");"
        self.connection.execute(sql)

    def write_paths(self):
      filename = 'web_paths.csv'
      f = self.api_db.joinpath(filename) 
      with open(f, "r") as file:
        csv_reader = csv.reader(file, delimiter=",")
        for line in csv_reader:
            # print(line[0])
            # print(line[1])
            id = line[0]
            folder_rel = line[1]
            path = "S3://atx-cloud-dev/data/"
            full = path + folder_rel + "/"
            dic = {"results_folder_path": full}
            self.edit_row("results_metadata",dic, "results_id",  id)

    def make_public(self):
        filename = "public_tissue_ids.csv"
        f = self.api_db.joinpath(filename)
        with open(f, "r") as file:
            lines  = file.readlines()
            for line in lines:
                 id = line.strip()
                 print(id)
                 dic = {
                    "public": True
                 }
                 self.edit_row("results_metadata", dic, "results_id", id)
    
    def add_descriptions(self):
        filename = "descriptions_titles.csv"
        f = self.api_db.joinpath(filename)
        with open(f, "r") as file:
            reader = csv.reader(file, delimiter=",")
            for line in reader:
                id = line[1]
                title = line[2]
                description = line[3]
                change_dict = {
                    "result_title": title,
                    "result_description": description
                }
                self.edit_row("results_metadata", change_dict, "results_id", id)

    
    def set_groups_file(self):
        file_name = "set_results_groups.csv"
        f = self.api_db.joinpath(file_name)
        with open(f, "r") as file:
            reader = csv.reader(file, delimiter=",")
            for line in reader:
                result_id = line[0].strip()
                group = line[1].strip()
                dic = {"`group`": group}
                self.edit_row("results_metadata", dic, "results_id", result_id)
    
    def set_groups_from_source(self):
        dic = {
            "Pieper": "Pieper",
            "Rai": "Rai",
            "Mt_Sinai": "Hurd",
        }
        sql = "SELECT results_id, tissue_source FROM tissue_results_merged;"
        res = self.connection.execute(sql)
        ids_source = res.fetchall()
        for item in ids_source:
            id = item[0]
            og_source = item[1]
            if og_source in dic.keys():
                group = dic[og_source]
                change_dict = {"`group`": group}
                self.edit_row("results_metadata", change_dict, "results_id", id)

        # file = open("group_names_from_source.csv")
        # group_mapping = json.load(file)
        # print(group_mapping)



    def delete_row(self, table_name, on_var, on_var_value):
        print(f"deleting: {on_var} = {on_var_value}")

    def get_epitope_to_id_dict(self):
        sql = "SELECT epitope, antibody_id FROM antibodies;"
        sql_obj = self.connection.execute(sql)
        tuple_list = sql_obj.fetchall()
        antibody_dict = {x[0]: x[1] for x in tuple_list}
        return antibody_dict

    def get_ngs_ids(self):
        sql = '''SELECT ngs_id FROM dbit_metadata WHERE web_object_available = 1;  
        '''
        res = self.connection.execute(sql)
        ids = res.fetchall()
        ids_final = []
        for i in range(len(ids)):
            ng_id = ids[i][0]
            ids_final.append({'id': ng_id})
        return ids_final

    def write_update(self ,status):
        sql1 =  "SELECT MAX(inx) FROM dbit_data_repopulations;"
        res = self.connection.execute(sql1)
        prev_inx = res.fetchone()[0]
        new_inx = prev_inx + 1
        current_date = str(datetime.datetime.now())
        period_inx = current_date.find('.')
        current_date = current_date[:period_inx]
        sql = """INSERT INTO dbit_data_repopulations(inx, date, result)
                VALUES({inx}, '{date}', '{result}');""".format(inx = new_inx, date = current_date, result = status)
        self.connection.execute(sql)
    def getColumns(self, run_ids, columns,on_var ,table):
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
            sql3 = " WHERE {identification_var} in {id_list};".format(identification_var = on_var ,id_list = tup)
        elif len(run_ids) == 1:
            sql3 = " WHERE {identification_var} = '{on}';".format(identification_var = on_var, on = run_ids[0])
        else:
            sql3 = ";"
        sql = sql1 + sql2 + sql3
        engine_result = self.connection.execute(sql)
        result = engine_result.fetchall()
        result_final = result[0]
        result_dict = self.list_to_dict(result_final, columns)
        return result_dict

    def getCollaboratorRuns(self, table, collaborator, web_objs_only):
        sql1 = "SELECT * FROM " + str(table)
        sql2 = " WHERE tissue_source = '{}'".format(collaborator)
        if web_objs_only:
            sql3 = " AND web_object_available = 1;"
        else:
            sql3 = ";"
        sql = sql1 + sql2 + sql3
        executed_result = self.connection.execute(sql)
        result_all = executed_result.fetchall()
        sql_col_names = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = N'{}';""".format(table)
        cols_result = self.connection.execute(sql_col_names)
        cols = cols_result.fetchall()
        cols = [x[0] for x in cols]
        result_dict = self.list_of_dicts(result_all, cols)
        return result_dict

    def getRuns(self, table, group):
      sql1 = "SELECT * FROM {}".format(table)
      if group:
        endString = group.split(',')
        sql2 = " WHERE "
        for i in endString:
          sql2 += "run_id = \'{}\' AND ".format(i)
        sql2 = sql2[:-4]
        sql2 += ';'
      else:
        sql2 = ';'
      sql = sql1 + sql2
      executed_result = self.connection.execute(sql)
      result_all = executed_result.fetchall()
      sql_col_names = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                          WHERE TABLE_NAME = N'{}';""".format(table)
      cols_result = self.connection.execute(sql_col_names)
      cols = cols_result.fetchall()
      cols = [x[0] for x in cols]
      result_dict = self.list_of_dicts(result_all, cols)
      return result_dict 

    def list_to_dict(self, lis, cols):
        final_dict = {}
        for i in range(len(cols)):
            final_dict[cols[i]] = lis[i]
        return final_dict

    def list_of_dicts(self, lis, cols):
        final_lis = []
        for item in lis:
            sub_dict = {}
            for i in range(len(item)):
                sub_dict[cols[i]] = item[i]
            final_lis.append(sub_dict)
        return final_lis
    
    def pull_table(self, table_name):
            #obtain table
        user = self.auth.app.config['SLIMS_USERNAME']
        passw = self.auth.app.config['SLIMS_PASSWORD']
        endpoint = "https://slims.atlasxomics.com/slimsrest/rest/" + table_name
        pl = {}
        headers = {'Content-Type': 'application/json'}
        tab = requests.get(endpoint,headers=headers,auth=HTTPBasicAuth(user, passw))
        data = tab.json()
        # identify all the columns present throughout the entire table
        col_names = set()
        col_names.add("pk")
        for i in range(len(data["entities"])):
            ele = data["entities"][i]
            cols = ele["columns"]
            primary_key = ele["pk"]
            for item in cols:
                if item["name"] not in col_names:
                    col_names.add(item["name"])

        #dict to be turned into df
        dict_df = {}
        dict_df1 = {}
        #dict to check all columns have values
        col_checker = {}
        # setting datastructures
        for col in col_names:
            dict_df[col] = []
            dict_df1[col] = []
            col_checker[col] = []
        # populdate dictionaries with column values for each 
        for item in data["entities"]:
            self.reset_dict(col_checker, False)
            col_checker["pk"] = True
            pk = item["pk"]
            dict_df["pk"].append(pk)
            dict_df1["pk"].append(pk)
            for attr in item["columns"]:
                name = attr["name"]
                # setting displayvalue as blank and checking if something actually exists for it
                disp_val = ""
                if "displayValue" in attr and attr["displayValue"] != None:
                    disp_val = str(attr["displayValue"])
                
                val = str(attr["value"])
                concatted = val + "$%$" + disp_val
                dict_df[name].append(val)
                dict_df1[name].append(concatted)
                col_checker[name] = True
            
            for col in col_names:
                if col_checker[col] == False:
                    dict_df[col].append("")
                    dict_df1[col].append("")
        

        df = pd.DataFrame(dict_df) 
        df1 = pd.DataFrame(dict_df1)
        return (df, df1)


    def reset_dict(self, dic, val):
        for key in dic.keys():
            dic[key] = val

    def convert_to_display(self, df_to_change, df_mixed ,colname):
        mapping = self.get_mapping_var_renaming_dict(df_mixed, colname)
        df_to_change.replace({colname: mapping}, inplace = True)

    def map_vals_dict(self, df, colname, dict):
        # df.loc[df[colname] in dict.keys(), colname] = dict[]
        df = df.replace({colname: dict})
        return df

    def get_mapping_var_renaming_dict(self, df_mixed, col_name):
        sub = df_mixed[[col_name]]
        mapping = {}
        for index, row in sub.iterrows():
            ele = str(row[0])
            inx = ele.find("$%$")
            val = ele[: inx]
            disp_val = ele[inx + 3:]
            if inx >= 0:
                if val in mapping:
                    if disp_val != mapping[val]:
                        print("ERROR with " + val)
                else:
                    mapping[val] = disp_val 
        for key in mapping.keys():
            orig = mapping[key]
            new = str(orig).lower().replace(" ", "_")
            mapping[key] = new
        return mapping


    def convert_dates(self, df, colname):
        df[colname] = df[colname].map(lambda epoch_date: datetime.datetime.fromtimestamp(epoch_date // 1000).strftime('%Y-%m-%d %H:%M:%S'))
        return df
    def createPublicTable_old(self):
      filename = 'Adatabase.xlsx'
      f = self.api_db.joinpath(filename)
      df_dict = pd.read_excel(open(f, 'rb'), sheet_name=None)
      keyValue = {'Publication': 'publication_data', 'Run': 'public_dbit_metadata','authors': 'authors', 'author_publications': 'author_publications' }
      for i in list(df_dict.keys()):
        self.write_df(df_dict.get(i), keyValue[i])
      return {'response': 'Success'}

    def createPublicTable(self, antibody_dict):
        filename = "Adatabase.xlsx"
        f = self.api_db.joinpath(filename)
        df_dict = pd.read_excel(open(f, 'rb'), sheet_name=None)
        df_publication = df_dict["Publication"]
        df_run = df_dict["Run"]
        df_authors = df_dict['authors']
        df_author_publications = df_dict["author_publications"]

        # sql = "SELECT MAX(tissue_id) FROM tissue_slides;"
        # res = self.connection.execute(sql)
        # tup = res.fetchone()
        # print(tup)
        self.create_public_table_runs(df_run, antibody_dict)
        self.create_public_table_publications(df_publication, df_authors, df_author_publications)

    def create_public_table_publications(self, df_publications, df_authors, df_author_publications):
        print("making publications")
        self.write_df(df_publications, "publications")
        self.write_df(df_authors, "authors")
        self.write_df(df_author_publications, "author_publications")

    def create_public_table_runs(self, df_run, antibody_dict):
        tissue_slide_df = df_run.copy()
        tissue_cols = ["species", "tissue_type", "organ", "experimental_condition"]
        tissue_slide_df = tissue_slide_df[tissue_cols]
        sql = "SELECT MAX(tissue_id) FROM tissue_slides;"
        res = self.connection.execute(sql)
        tup = res.fetchone()
        max_id = tup[0]
        print(f"MAX: {max_id}")
        tissue_slide_df.to_sql("tissue_slides", self.engine, index=False, if_exists="append")

        metadata_df = df_run.copy()
        length = metadata_df.shape[0]
        ids = range(max_id + 1, max_id + length + 1)
        antibody_dict["None"] = pd.NA
        metadata_df = metadata_df.replace({"antibody": antibody_dict})
        metadata_df["tissue_id"] = ids
        metadata_df_cols = ["tissue_id", "assay", "publication_id", "group", "antibody", "result_title", "result_description", "resolution"]
        metadata_df = metadata_df[metadata_df_cols]
        renaming ={
            "antibody": "antibody_id",
            "resolution": "channel_width"
        }
        metadata_df.rename(mapper=renaming, axis = 1, inplace=True)
        true_list = [True] * metadata_df.shape[0]
        metadata_df["web_object_available"] = true_list
        metadata_df["public"] = true_list
        # metadata_df["antibody_id"] = pd.to_numeric(metadata_df['antibody_id'])
        metadata_df.to_sql("results_metadata", self.engine, index=False, if_exists="append")


    
    def create_meta_table(self, df_content, df_content_mixed):
        df_content = df_content.astype({"cntn_fk_status": str, "cntn_fk_contentType": str, "cntn_createdOn": int, "cntn_fk_status": str, "cntn_cf_fk_workflow": str})
        ngs_cols = df_content
        #taking all ngs libraries that have runids and are sequenced
        ngs = ngs_cols[(ngs_cols.cntn_fk_status == '55') & (ngs_cols.cntn_cf_runId.notnull())& (ngs_cols.cntn_cf_runId != "None") & (ngs_cols.cntn_fk_contentType == '5')]
        slides = df_content[(df_content.cntn_fk_contentType == '42') & (df_content.cntn_fk_status != '54')]
        ngs_slide = pd.merge(left=ngs, right=slides, on="cntn_cf_runId", how="inner", suffixes=("_NGS", ""))
        block_cols = df_content 
        tissue_block = block_cols[(block_cols.cntn_fk_contentType == '41')]

        ngs_slide["cntn_fk_originalContent"] = ngs_slide["cntn_fk_originalContent"].replace(["None"], -1)
        ngs_slide["cntn_fk_originalContent"] = pd.to_numeric(ngs_slide["cntn_fk_originalContent"])

        block_ngs = pd.merge(left=ngs_slide, right=tissue_block, left_on="cntn_fk_originalContent", right_on="pk", how = "left", suffixes=("", "_block"))

        species_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_species")
        species_mapping['53'] = 'homo_sapiens'
        organ_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_organ")
        assay_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_workflow")
        tissueType_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_tissueType")
        epitope_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_epitope")
        block_ngs = self.map_vals_dict(block_ngs, "cntn_cf_fk_species", species_mapping)
        block_ngs = self.map_vals_dict(block_ngs, "cntn_cf_fk_organ", organ_mapping)
        block_ngs = self.map_vals_dict(block_ngs, "cntn_cf_fk_tissueType", tissueType_mapping)
        block_ngs = self.map_vals_dict(block_ngs, "cntn_cf_fk_workflow", assay_mapping)
        block_ngs = self.map_vals_dict(block_ngs, "cntn_cf_fk_epitope", epitope_mapping)
        for i, row in block_ngs.iterrows():
            if row["cntn_cf_fk_workflow"] == "cut_n_tag":
                block_ngs.at[i, "cntn_cf_fk_workflow"] = row["cntn_cf_fk_epitope"] 

        block_ngs = self.convert_dates(block_ngs, "cntn_createdOn_NGS")
        web_objs = self.get_web_objs_ngs()
        web_obs_vals = []
        for val in block_ngs["cntn_id_NGS"]:
            if val in web_objs:
                web_obs_vals.append(True)
            else:
                web_obs_vals.append(False)
        cols = ["cntn_id_NGS", "cntn_cf_runId", "cntn_createdOn_NGS", "cntn_cf_fk_tissueType","cntn_cf_fk_organ", "cntn_cf_fk_species", "cntn_cf_experimentalCondition", "cntn_cf_sampleId", "cntn_cf_tissueSlideExperimentalCondition", "cntn_cf_source", "cntn_cf_fk_workflow"]
        tissue = block_ngs[cols].copy()
        tissue["web_object_available"] = web_obs_vals
        rename_mapping = {
            "cntn_id_NGS": "ngs_id",
            "cntn_cf_runId": "run_id",
            "cntn_createdOn_NGS": "created_on",
            "cntn_cf_fk_tissueType": "tissue_type",
            "cntn_cf_fk_organ": "organ_name",
            "cntn_cf_fk_species": "species",
            "cntn_cf_experimentalCondition": "experimental_condition",
            "cntn_cf_sampleId": "sample_id",
            "cntn_cf_tissueSlideExperimentalCondition": "tissue_slide_experimental_condition",
            "cntn_cf_source": "tissue_source",
            "cntn_cf_fk_workflow": "assay"
        }
        tissue.rename(mapper=rename_mapping, axis=1, inplace=True)
        # tissue.loc[tissue["cntn_id_NGS"] in web_objs, 'web_object_available'] = True
        return tissue
    
    def get_web_objs_ngs(self):
        aws_resp = self.aws_s3.list_objects_v2(
            Bucket = self.bucket_name,
            Prefix = "data/"
        )
        files = aws_resp['Contents']
        h5ad_files = [x['Key'] for x in files if 'genes.h5ad' in x['Key']]
        ng_ids = set()
        for file in h5ad_files:
            lis = file.split("/")
            ng_id = lis[1].strip()
            ng_ids.add(ng_id)
        return ng_ids



    def create_bfx_table(self, df_content, df_results):
        bfx_res = df_results
        df_content = df_content.astype({"cntn_cf_runId": str, "cntn_fk_status": str, "pk": str})
        bfx_res = bfx_res.astype({"rslt_fk_test": str, "rslt_fk_content": str})
        bfx_res = bfx_res[bfx_res.rslt_fk_test == '39']
        df_content["cntn_fk_status"] = df_content["cntn_fk_status"]
        filt_content = df_content[(df_content.cntn_cf_runId != "nan") & (df_content.cntn_cf_runId != "None") & (df_content.cntn_fk_status == '55')]

        # # # ngs = ngs_cols[(ngs_cols.cntn_fk_status == 55) & (ngs_cols.cntn_cf_runId.notnull())& (ngs_cols.cntn_cf_runId != "None") & (ngs_cols.cntn_fk_contentType == 5)]
        content_bfx = pd.merge(left=bfx_res, right=filt_content, left_on="rslt_fk_content", right_on="pk", how="inner")
        cols = ["cntn_cf_runId", "cntn_id", "rslt_createdOn", "rslt_cf_rawNumberOfReads1", "rslt_cf_refGenome", "rslt_cf_pipelineVersion", "rslt_cf_estimatedNumberOfCells", "rslt_cf_confidentlyMappedReadPairs", "rslt_cf_estimatedBulkLibraryComplexity1", "rslt_cf_fractionOfGenomeInPeaks", "rslt_cf_fractionOfHighQualityFragmentsInCells", "rslt_cf_fractionOfHighQualityFragmentsOverlap", "rslt_cf_fractionOfHighQualityFragmentsOrlapPe", "rslt_cf_fractionOfTranspositionEventsInPeaksI", "rslt_cf_fragmentsFlankingASingleNucleosome", "rslt_cf_fragmentsInNucleosomeFreeRegions", "rslt_cf_meanRawReadPairsPerCell1", "rslt_cf_medianHighQualityFragmentsPerCell", "rslt_cf_nonNuclearReadPairs", "rslt_cf_numberOfPeaks", "rslt_cf_percentDuplicates", "rslt_cf_q30BasesInBarcode", "rslt_cf_q30BasesInRead1", "rslt_cf_q30BasesInRead2", "rslt_cf_q30BasesInSampleIndexI1", "rslt_cf_sequencedReadPairs1", "rslt_cf_sequencingSaturation", "rslt_cf_tssEnrichmentScore", "rslt_cf_unmappedReadPairs", "rslt_cf_validBarcodes", "rslt_cf_fragmentsPercentOffTissue", "rslt_cf_fragmentsAverageOffTissue", "rslt_cf_fragmentsStandardDeviationOffTissue", "rslt_cf_fragmentsMaxOffTissue", "rslt_cf_fragmentsMinOffTissue", "rslt_cf_numberOfTixelsOnTissue", "rslt_cf_fragmentsAverageOnTissue", "rslt_cf_fragmentsStandardDeviationOnTissue", "rslt_cf_fragmentsMaxOnTissue", "rslt_cf_fragmentsMinOnTissue", "rslt_cf_medianTssScore"]
        content_bfs_cols= content_bfx[cols]
        content_bfs_cols = content_bfs_cols.drop_duplicates(
            subset = ["cntn_cf_runId", "cntn_id"],
            keep = "last").reset_index(drop=True)
        renaming_dict = {
            "cntn_cf_runId": "run_id",
            "cntn_id": "ngs_id",
            "rslt_createdOn": "result_created_on",
            "rslt_cf_rawNumberOfReads1": "raw_read_count",
            "rslt_cf_refGenome": "reference_genome",
            "rslt_cf_pipelineVersion": "pipeline_version",
            "rslt_cf_estimatedNumberOfCells": "estimated_number_cells",
            "rslt_cf_confidentlyMappedReadPairs": "confidently_mapped_readpairs",
            "rslt_cf_estimatedBulkLibraryComplexity1": "estimated_bulk_library_complexity",
            "rslt_cf_fractionOfGenomeInPeaks": "fraction_genome_in_peaks",
            "rslt_cf_fractionOfHighQualityFragmentsInCells": "fraction_high_quality_fragments_in_cells",
            "rslt_cf_fractionOfHighQualityFragmentsOverlap": "fraction_high_quality_fragments_overlap",
            "rslt_cf_fractionOfHighQualityFragmentsOrlapPe": "fraction_high_quality_fragments_overlap_peaks",
            "rslt_cf_fractionOfTranspositionEventsInPeaksI": "fraction_transpos_peaks_cells",
            "rslt_cf_fragmentsFlankingASingleNucleosome" : "fragments_flanking_single_nucleosome",
            "rslt_cf_fragmentsInNucleosomeFreeRegions" : "fragments_nucleosome_free_region",
            "rslt_cf_meanRawReadPairsPerCell1": "mean_raw_readpairs_percell",
            "rslt_cf_medianHighQualityFragmentsPerCell": "median_high_quality_fragments_per_cell",
            "rslt_cf_nonNuclearReadPairs" : "non_nuclear_read_pairs",
            "rslt_cf_numberOfPeaks" : "number_of_peaks",
            "rslt_cf_percentDuplicates" : "percent_duplicates",
            "rslt_cf_q30BasesInBarcode" : "q30_bases_in_barcode",
            "rslt_cf_q30BasesInRead1" : "q30_bases_in_read",
            "rslt_cf_q30BasesInRead2" : "q30_bases_in_read_2",
            "rslt_cf_q30BasesInSampleIndexI1": "q30_bases_in_sample_indexI1",
            "rslt_cf_sequencedReadPairs1" : "sequenced_read_pairs",
            "rslt_cf_sequencingSaturation" : "sequencing_saturation",
            "rslt_cf_tssEnrichmentScore" : "tss_enrichment_score",
            "rslt_cf_unmappedReadPairs" : "unmapped_read_pairs",
            "rslt_cf_validBarcodes" : "valid_barcodes",
            "rslt_cf_fragmentsPercentOffTissue" : "fragments_percent_off_tissue",
            "rslt_cf_fragmentsAverageOffTissue" : "fragments_average_off_tissue",
            "rslt_cf_fragmentsStandardDeviationOffTissue" : "fragments_standard_deviation",
            "rslt_cf_fragmentsMaxOffTissue" : "fragments_max_off_tissue",
            "rslt_cf_fragmentsMinOffTissue" : "fragments_min_off_tissue",
            "rslt_cf_numberOfTixelsOnTissue" : "tixels_on_tissue",
            "rslt_cf_fragmentsAverageOnTissue": "fragments_average_on_tissue",
            "rslt_cf_fragmentsStandardDeviationOnTissue" : "fragments_standard_deviation_on_tissue",
            "rslt_cf_fragmentsMaxOnTissue" : "fragments_max_on_tissue",
            "rslt_cf_fragmentsMinOnTissue" : "fragments_min_on_tissue",
            "rslt_cf_medianTssScore": "median_tss_score"
        }
        content_bfs_cols.rename(mapper=renaming_dict, axis=1, inplace=True)
        content_bfs_cols = content_bfs_cols.replace(['None'], '')

        # content_bfs_cols[['raw_read_count', 'q30_bases_in_sample_indexI1']] = content_bfs_cols[['raw_read_count', 'q30_bases_in_sample_indexI1']].apply(lambda x: self.remove_none(x1, x2))
        non_numeric_cols = ['run_id', 'ngs_id', 'result_created_on', 'reference_genome', 'pipeline_version']
        numeric_cols = content_bfs_cols.columns[~content_bfs_cols.columns.isin(non_numeric_cols)]
        content_bfs_cols[numeric_cols] = content_bfs_cols[numeric_cols].apply(pd.to_numeric)

        return content_bfs_cols

    def create_flow_table(self, df_content, df_result, df_experiment_rs):
        # ngs = df_content[(df_content.cntn_fk_status == 55) & (df_content.cntn_cf_runId.notnull())& (df_content.cntn_cf_runId != "None") & (df_content.cntn_fk_contentType == 5)]
        cols = ["cntn_cf_runId", "cntn_id", "cntn_pk"]
        # filtering for tissue slides with a not empty run ID
        df_content = df_content.astype({"cntn_fk_contentType": str})
        tissue_slides = df_content[(df_content["cntn_fk_contentType"] == '42') & (df_content.cntn_cf_runId.notnull()) & (df_content.cntn_cf_runId != "None")]
        tissue_slides = tissue_slides[cols]
        
        #filtering for results that have a test of Flow QC
        df_result = df_result.astype({"rslt_fk_test": str})
        result_flowqc = df_result[df_result["rslt_fk_test"] == '33']
        
        ligations_1 = df_experiment_rs[df_experiment_rs["xprs_name"] == "Ligation 1"]
        # ligations_1["xprs_pk"] = ligations_1["xprs_pk"].astype(str)
        ligations_1 = ligations_1.astype({"xprs_pk": str})
        # ligations_1.loc[:, "xprs_pk"] = ligations_1["xprs_pk"].astype(str)

        ligations_1_joined = pd.merge(left = result_flowqc, right=ligations_1, left_on="rslt_fk_experimentRunStep", right_on="xprs_pk", how="inner")
        cols1 = ["rslt_cf_leak", "rslt_cf_fk_blocks", "rslt_cf_fk_leaks", "rslt_cf_flowTime", "xprs_name", "rslt_fk_content", "rslt_fk_test", "test_label"]
        ligations_1_joined = ligations_1_joined[cols1]
        content_ligation1 = pd.merge(left=tissue_slides, right=ligations_1_joined, left_on="cntn_pk", right_on="rslt_fk_content", how="inner")
        cols1.extend(["cntn_cf_runId", "cntn_pk"])
        content_ligations1 = content_ligation1[cols1]

        ligations_2 = df_experiment_rs[df_experiment_rs["xprs_name"] == "Ligation 2"]
        ligations_2 = ligations_2.astype({"xprs_pk": str})
        # ligations_2["xprs_pk"] = ligations_2["xprs_pk"].astype(str)
        ligations_2_joined = pd.merge(left = result_flowqc, right=ligations_2,left_on="rslt_fk_experimentRunStep", right_on="xprs_pk", how="inner")
        content_ligations2 = pd.merge(left=tissue_slides, right=ligations_2_joined, left_on="cntn_pk", right_on="rslt_fk_content", how="inner")
        content_ligations2 = content_ligations2[cols1]
        
        flow_joined = pd.merge(left=content_ligations1, right=content_ligations2, on="cntn_cf_runId", how="inner", suffixes=("_a", "_b"))
        rename_mapping = {
            "cntn_cf_runId": "run_id",
            "rslt_cf_leak_a": "leak_a",
            "rslt_cf_leak_b": "leak_b",
            "rslt_cf_fk_blocks_a": "blocks_a",
            "rslt_cf_fk_blocks_b": "blocks_b",
            "rslt_cf_fk_leaks_a": "crosses_a",
            "rslt_cf_fk_leaks_b": "crosses_b",
            "rslt_cf_flowTime_a": "flowtime_a",
            "rslt_cf_flowTime_b": "flowtime_b",
        }
        cols = ["cntn_cf_runId", "rslt_cf_leak_a", "rslt_cf_leak_b", "rslt_cf_fk_blocks_a", "rslt_cf_fk_blocks_b", "rslt_cf_fk_leaks_a", "rslt_cf_fk_leaks_b", "rslt_cf_flowTime_a", "rslt_cf_flowTime_b"]
        flow_joined = flow_joined[cols]
        flow_joined.rename(mapper = rename_mapping, axis=1, inplace=True)
        return flow_joined

    def grab_tissue_information(self, content, content_mixed):
        copy_content_1 = content.copy()
        copy_content_1 = copy_content_1.astype({"cntn_fk_contentType": str})
        tissue_slides = copy_content_1[(copy_content_1.cntn_fk_contentType == '42') & (copy_content_1.cntn_cf_runId.notnull()) & (copy_content_1.cntn_cf_runId != "None") & (copy_content_1.cntn_fk_status != 54) & (copy_content_1.cntn_cf_fk_workflow.isin(["4","7","10"]))]
        tissue_slides["cntn_cf_fk_chipB"] = tissue_slides["cntn_cf_fk_chipB"].astype(str, copy=True)
        content_copy = content.copy()
        content_copy["pk"] = content_copy["pk"].astype(str, copy=True)

        tissue_chipb = pd.merge(left = tissue_slides, right = content_copy, how="left" ,left_on="cntn_cf_fk_chipB", right_on="pk", suffixes=("", "_bchip"))

        cols = ["cntn_cf_source","cntn_cf_roiChannelWidthUm_bchip", "cntn_cf_fk_tissueType", "cntn_cf_fk_species","cntn_cf_sampleId","cntn_cf_fk_organ","cntn_cf_experimentalCondition","cntn_cf_runId", "cntn_cf_fk_workflow", "cntn_cf_fk_epitope", "cntn_cf_fk_regulation"]
        tissue_slides = tissue_chipb[cols]
        tissue_slides.to_csv("tissue_slides_roi.csv")

        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_tissueType")
        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_species")
        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_workflow")
        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_epitope")
        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_regulation")
        self.convert_to_display(tissue_slides, content_mixed, "cntn_cf_fk_organ")

        rename_mapping = {
            "cntn_cf_fk_tissueType": "tissue_type",
            "cntn_cf_source": "tissue_source",
            "cntn_cf_fk_species": "species",
            "cntn_cf_fk_workflow": "assay",
            "cntn_cf_fk_epitope": "epitope",
            "cntn_cf_fk_regulation": "regulation",
            "cntn_cf_sampleId": "sample_id",
            "cntn_cf_experimentalCondition": "experimental_condition",
            "cntn_cf_fk_organ": "organ",
            "cntn_cf_runId": "run_id",
            "cntn_cf_roiChannelWidthUm_bchip": "channel_width"
        }
        tissue_slides.rename(mapper=rename_mapping, axis=1, inplace=True)
        tissue_slides["tissue_id"] = pd.RangeIndex(start = 0, stop = tissue_slides.shape[0])
        tissue_slides["channel_width"] = pd.to_numeric(tissue_slides["channel_width"], errors="coerce")
        print(tissue_slides.columns)
        return tissue_slides

    def get_tissue_slides_sql_table(self, tissue_df):
        tissue_table_cols = ["tissue_id", "tissue_source","species","tissue_type", "sample_id", "experimental_condition", "organ", "run_id"]
        tissue_slides_table = tissue_df[tissue_table_cols]
        return tissue_slides_table

    def create_antibody_table(self, tissue_db):
        antibody_dict = {}
        tissue_db.reset_index(inplace = True, drop = True)
        for (index, val) in enumerate(tissue_db["regulation"]):
            if val != "":
                epitope = tissue_db["epitope"][index]
                antibody_dict[epitope] = val
        antibody_df = pd.DataFrame(antibody_dict.items(), columns=["epitope", "regulation"])
        antibody_df["antibody_id"] = antibody_df.index
        return antibody_df

    def populate_antibody_table(self):
        df_dict = {
            "epitope" : ["h3k27me3", "h3k27ac", "h3k4me3"],
            "regulation": ["repression", "activation", "TBD"],
        }
        df = pd.DataFrame(data=df_dict)
        self.write_df(df=df, table_name="antibodies")


    def create_run_metadata_table(self, content, tissue_slides, antibody_dict):
        # tissue_slides = content[(content.cntn_fk_contentType == 42) & (content.cntn_cf_runId.notnull()) & (content.cntn_cf_runId != "None") & (content.cntn_fk_status != 54)]
        content_copy = content.copy()
        content_copy = content_copy.astype({"cntn_fk_contentType": str, "cntn_fk_status": str})

        ngs = content_copy[(content_copy.cntn_fk_contentType == '5') & (content_copy.cntn_cf_runId.notnull()) & (content_copy.cntn_cf_runId != "None") & (content_copy.cntn_fk_status == '55')]
        tissue_ngs = pd.merge(left = tissue_slides, right = ngs, left_on = "run_id", right_on = "cntn_cf_runId", how="left", suffixes=("", "_ngs"))
        tissue_ngs["results_id"] = pd.RangeIndex(start = 0, stop = tissue_ngs.shape[0])
        tissue_ngs.replace({"epitope": antibody_dict}, inplace=True)
        tissue_ngs["epitope"] = pd.to_numeric(tissue_ngs["epitope"])
        renaming = {
            "cntn_id": "ngs_id",
            "cntn_createdOn": "date",
            "epitope": "antibody_id"
        }
        tissue_ngs.rename(mapper=renaming,axis=1, inplace=True)
        cols_run_metadata = ["results_id","tissue_id", "date","antibody_id","assay", "ngs_id", "run_id", "channel_width"]
        run_metadata = tissue_ngs[cols_run_metadata]
        web_objects = self.get_web_objs_ngs()
        col = run_metadata.ngs_id.isin(web_objects)
        # run_identifiers = range(1, run_metadata.shape[0] + 1)
        run_metadata = run_metadata.assign(web_object_available=col, public = False, group = "AtlasXomics", publication_id = pd.NA)
        return run_metadata

    def create_atlas_runs_sql_table(self, tissue_ngs):
        ngs_run_id_cols = ["ngs_id", "run_id", "run_identifier"]
        atlas_runs = tissue_ngs[ngs_run_id_cols].copy()
        atlas_runs.loc[atlas_runs["ngs_id"] == " ", "ngs_id"] = pd.NA
        atlas_runs.dropna(subset="ngs_id", axis = 0, inplace=True)
        return atlas_runs

    def create_tables(self):
        meta = db.MetaData()
        antibody = db.Table(
            'antibodies', meta,
            db.Column('antibody_id', db.Integer, primary_key = True),
            db.Column("epitope", db.VARCHAR(length = 32), nullable = False),
            db.Column("regulation", db.VARCHAR(length = 32), nullable = False)
        )

        results_metadata = db.Table(
            "results_metadata", meta,
            db.Column("results_id", db.Integer, primary_key = True),
            db.Column("tissue_id", db.Integer),
            db.Column("ngs_id", db.VARCHAR(12)),
            db.Column("antibody_id", db.Integer),
            db.Column("assay", db.VARCHAR(length = 32)),
            db.Column("publication_id", db.Integer),
            db.Column("public", db.Boolean),
            db.Column("group", db.String(length = 100)),
            db.Column("web_object_available", db.Boolean),
            db.Column("date", db.BigInteger),
            db.Column("results_folder_path", db.String(200)),
            db.Column("channel_width", db.Integer),
            db.Column("result_title", db.String(500)),
            db.Column("result_description", db.String(500))
        )

        tissue_slide_table = db.Table(
            "tissue_slides", meta,
            db.Column("tissue_id", db.Integer, primary_key = True),
            db.Column("run_id", db.VARCHAR(length=100)),
            db.Column("tissue_source", db.VARCHAR(length = 100)),
            db.Column("species", db.VARCHAR(length = 100)),
            db.Column("organ", db.VARCHAR(length=100)),
            db.Column("tissue_type", db.VARCHAR(length = 100)),
            db.Column("sample_id", db.VARCHAR(length = 100)),
            db.Column("experimental_condition", db.String(length = 250)),
            db.Column("image_folder_path", db.String(200))
        )

        # atlas_runs = db.Table(
        #     "atlas_runs", meta,
        #     db.Column("run_identifier", db.Integer, primary_key = True),
        #     db.Column("ngs_id", db.VARCHAR(length = 32)),
        #     db.Column("atlas_run_id", db.VARCHAR(length = 32))
        # )

        publication_table = db.Table(
            "publications", meta,
            db.Column("publication_id", db.Integer, primary_key = True),
            db.Column("pmid", db.Integer),
            db.Column("journal", db.VARCHAR(32)),
            db.Column("date", db.Date),
            db.Column("title", db.TEXT),
            db.Column("link", db.String(200))
        )

        authors_publication_table = db.Table(
            "author_publications", meta,
            db.Column("publication_id", db.Integer, primary_key = True),
            db.Column("author_id", db.Integer, primary_key = True),
        )

        authors_table = db.Table(
            "authors", meta,
            db.Column("author_id", db.Integer, primary_key = True),
            db.Column("author_name", db.VARCHAR(100))
        )

        meta.create_all(self.engine)


    def write_df(self, df, table_name):
        sql = "DELETE FROM " + table_name + ";"
        self.connection.execute(sql)
        df.to_sql(table_name, self.engine, index=False, if_exists="append")

