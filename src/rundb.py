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
        self.homepage_population_name = "populate_homepage"
        self.full_db_data = "metadata_full_DB"

    def initialize(self):
        try:
            connection_string = "mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}".format(username=self.username, password=self.password, host=self.host, port=str(self.port), dbname=self.db)
            self.engine = db.create_engine(connection_string)
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

    def initEndpoints(self):
        @self.auth.app.route('/api/v1/run_db/reinitialize_db', methods=["GET"])
        def re_init():
            status_code = 200
            try:
                connection_string = "mysql+pymysql://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.db
                self.engine = db.create_engine(connection_string)
                res = "Success"
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
                params = request.get_json()
                table = params["table"]
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


        @self.auth.app.route("/api/v1/run_db/populate_homepage", methods=["GET"])
        @self.auth.login_required
        def _populate_homepage():
            sc = 200
            try:
                user, groups= current_user
                if not groups:
                    group = " "
                else:
                    group = groups[0]
                if group == 'admin' or group == 'user':
                    res = self.grab_runs_homepage_admin()
                else:
                    res = self.grab_runs_homepage_group(group)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                resp.headers['Content-Type']='application/json'
                return resp

        @self.auth.app.route("/api/v1/run_db/get_run_from_results_id", methods=["POST"])
        @self.auth.login_required
        def _get_run_metadata():
            sc = 200
            params = request.get_json()
            results_id = params['results_id']
            try:
                user, groups = current_user
                res = self.get_run(results_id, groups)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                resp.headers['Content-Type'] = 'application/json'
                return resp

        @self.auth.app.route("/api/v1/run_db/create_study", methods=["POST"])
        @self.auth.login_required
        def _create_study():
            sc = 200
            params = request.get_json()
            result_ids = params.get("result_ids", [])
            params.pop("result_ids", None)
            try:
                self.create_study(params, result_ids)
                res = "Success"
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{str(e)} {exc}")
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/search_authors", methods=["POST"])
        @self.auth.login_required
        def _search_authors():
            params = request.get_json()
            # table_name = params["table_name"]
            query = params["query"]
            on_var = "author_name"
            table_name = "author_search"
            try:
                sc = 200
                res = self.search_table(table_name, on_var, query)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_field_options", methods=['GET'])
        @self.auth.admin_required
        def _get_field_options():
            sc = 200
            try:
                res = self.get_field_options()
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_run_ids", methods=['GET'])
        @self.auth.login_required
        def _get_run_ids():
            sc = 200
            try:
                res = self.get_run_ids()
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/search_pmid", methods=["POST"])
        @self.auth.login_required
        def _search_pmid():
            params = request.get_json()
            # table_name = params["table_name"]
            table_name = "pmid_search"
            on_var = "pmid"
            query = params["query"]
            try:
                sc = 200
                res = self.search_table(table_name, on_var, query)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/retrieve_paths", methods=["GET"])
        @self.auth.login_required
        def _retrieve_paths():
            status_code = 200
            try:
                user, groups = current_user
                if not groups:
                    group = " "
                else:
                    group = groups[0]
                if group == "admin" or group == "user":
                    res = self.get_paths_admin()
                else:
                    res = self.get_paths_group(group)
            except Exception as e:
                print(e)
                status_code = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), status=status_code)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_summary_stats", methods=["GET"])
        @self.auth.login_required
        def _get_summary_stats():
            sc = 200
            try:
                user, groups = current_user
                print(groups)
                if not groups:
                    group = ""
                else:
                    group = groups[0]
                
                if group == "admin" or group == "user":
                    res = self.grab_summary_stat_admin()
                else:
                    res = self.grab_summary_stats(group)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_info_from_run_id", methods=["POST"])
        @self.auth.login_required
        def _get_info_from_run_id():
            sc = 200
            data = request.get_json()
            run_id = data["run_id"]
            try:
                res = self.get_info_from_run_id(run_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_info_from_results_id", methods=['POST'])
        @self.auth.admin_required
        def _get_info_from_results_id():
            sc = 200
            data = request.get_json()
            results_id = data["results_id"]
            try:
                res = self.get_info_from_results_id(results_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
            finally:
                resp = Response(json.dumps(res), sc)
                print(resp)
                return resp

        @self.auth.app.route("/api/v1/run_db/upload_metadata_page", methods=["POST"])
        @self.auth.admin_required
        def _upload_metadata_page():
            sc = 200
            values = request.get_json()
            print(values)
            try:
                self.check_def_tables(values)
                self.write_web_obj_info(values)
                res = "Success"
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{str(e)} {exc}", sc)
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/update_db_slims_info", methods=["POST"])
        @self.auth.admin_required
        def _update_tables():
            sc = 200
            try:
                self.update_db_slims()
                res = "Success"
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{str(e)} {exc}", sc)
            finally:
                resp = Response(json.dumps(res), sc)
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


    def update_db_slims(self):
        df_dict = self.get_sql_ready_tables_slims()
        tissue_slides = df_dict["tissue_slides_sql"]
        results_meta = df_dict["run_metadata_sql"]
        tissue_slides.to_csv("tissue_slides.csv")
        results_meta.to_csv("results_metadata.csv")

        tissue_slide_cols = ["run_id", "tissue_source", "species", "organ", "tissue_type", "sample_id", "experimental_condition"]
        ## USE RUN_ID###
        self.update_db_table("tissue_slides", tissue_slides, tissue_slide_cols,"tissue_id")

        sql = "SELECT MIN(results_id) FROM results_metadata where {INSERT_VAR} = 'AtlasXomics';".format("f")
        # results_metadata_cols = ["assay", "date", "channel_width"]
        # self.update_db_table("results_metadata", results_meta, results_metadata_cols, "results_id")

    def grab_summary_stats(self, group):
        conn = self.engine.connect()
        sql = f"""SELECT assay as variable, count(assay) as count FROM {self.homepage_population_name} WHERE (`group` = '{group}' OR public = 1) group by assay
                    UNION SELECT `group` as variable, count(`group`) as count FROM {self.homepage_population_name} WHERE (`group` = '{group}' OR public = 1) group by `group`"""
        sql_obj = conn.execute(sql)
        res = sql_obj.fetchall()

        result = {x[0]: x[1] for x in res}
        return result

    def grab_summary_stat_admin(self):
        conn = self.engine.connect()
        sql = f"""SELECT assay as variable, count(assay) as count FROM {self.homepage_population_name} group by assay
                    UNION SELECT `group` as variable, count(`group`) as count FROM {self.homepage_population_name} group by `group`"""
        sql_obj = conn.execute(sql)
        res = sql_obj.fetchall()

        result = {x[0]: x[1] for x in res}
        return result

    def write_web_obj_info(self, values):
        mapping_dict = self.get_def_table_mappings()
        species = values.get("species", None)
        species_id = mapping_dict['species'].get(species, None)
        assay= values.get("assay", None)
        assay_id = mapping_dict['assay'].get(assay, None)
        organ= values.get("organ", None)
        organ_id = mapping_dict['organ'].get(organ, None)
        group = values.get("group", None)
        group_id = mapping_dict['group'].get(group, None)
        pmid = values.get("pmid", None)
        publication_id = mapping_dict["publication"].get(pmid, None)

        antibody = values.get("epitope", None)
        antibody_id = mapping_dict['antibody'].get(antibody, None)

        tissue_source = values.get("tissue_source", None)
        tissue_source_id = mapping_dict['tissue_source'].get(tissue_source, None)

        run_id = values.get("run_id", None)
        tissue_type = values.get("tissue_type", None)
        sample_id = values.get("sample_id", None)
        experimental_condition = values.get("experimental_condition", None)
        channel_width = values.get("channel_width", None)
        if not channel_width:
            channel_width = None
        number_channels = values.get("number_channels", None)
        if not number_channels:
            number_channels = None

        date = values.get("date", None)

        web_obj_path = values.get("web_obj_path", None)

        web_obj_available = False
        if web_obj_path:
            web_obj_available = True

        public = values.get("public", False)
        result_title = values.get("run_title", None)
        result_description = values.get("run_description")
        ngs_id = values.get("ngs_id", None)
        result_date = values.get("date", None)
        results_id = values.get("results_id", None)

        tissue_dict = {
            "organ_id": organ_id,
            "species_id": species_id,
            "tissue_source_id": tissue_source_id,
            "run_id": run_id,
            "sample_id": sample_id,
            "experimental_condition": experimental_condition,
            "assay_id": assay_id,
            "antibody_id": antibody_id,
            "tissue_type": tissue_type,
            "channel_width": channel_width,
            "number_channels": number_channels
        }
        result_dict = {
            "publication_id": publication_id,
            "web_object_available": web_obj_available,
            "results_folder_path": web_obj_path,
            "result_title": result_title,
            "result_description": result_description,
            "public": public,
            "group_id": group_id,
            "ngs_id": ngs_id,
            "result_date": result_date,
            "date": date
        }
        #check if run_id is present in tissue_slides
        conn = self.engine.connect()
        sql_check_existence = f"""SELECT tissue_id FROM tissue_slides WHERE run_id = '{run_id}';"""
        obj = conn.execute(sql_check_existence)
        ele = obj.fetchone()
        
        if ele:
            self.edit_row("tissue_slides", tissue_dict, "run_id", run_id)
        else:
            self.write_row("tissue_slides", tissue_dict)
        if results_id:
            self.edit_row("results_metadata", result_dict, "results_id", results_id)
        else:
            conn = self.engine.connect()
            sql_get_tissue_id = """SELECT tissue_id FROM tissue_slides WHERE run_id = %s;"""
            tup = (run_id,)
            sql_obj = conn.execute(sql_get_tissue_id, tup)
            id = sql_obj.fetchone()[0]
            result_dict['tissue_id'] = id
            self.write_row("results_metadata", result_dict)

    def get_def_table_mappings(self):
        result = {}
        conn = self.engine.connect()
        sql_assay = """SELECT * FROM assay_table"""
        obj_assay = conn.execute(sql_assay)
        assay_map = {x[1]: x[0] for x in obj_assay.fetchall()}
        result["assay"] = assay_map
         
        sql_species = """SELECT * FROM species_table"""
        obj_species = conn.execute(sql_species)
        species_map = {x[1]: x[0] for x in obj_species.fetchall()}
        result['species'] = species_map

        sql_organ = """SELECT * FROM organ_table"""
        obj_organ = conn.execute(sql_organ)
        organ_map = {x[1]: x[0] for x in obj_organ.fetchall()}
        result["organ"] = organ_map

        sql_antibody = """SELECT * FROM antibody_table"""
        obj_antibody = conn.execute(sql_antibody)
        antibody_map = {x[1]: x[0] for x in obj_antibody.fetchall()}
        result["antibody"] = antibody_map

        sql_publication = """SELECT publication_id, pmid FROM publications;"""
        obj_publication = conn.execute(sql_publication)
        publication_map = {x[1]: x[0] for x in obj_publication.fetchall()}
        result["publication"] = publication_map

        sql_tissue_source = """SELECT * FROM tissue_source_table;"""
        obj_tissue_source = conn.execute(sql_tissue_source)
        tissue_source_map = {x[1]: x[0] for x in obj_tissue_source.fetchall()}
        result["tissue_source"] = tissue_source_map
        
        sql_group = """SELECT * FROM groups_table;"""
        obj_group = conn.execute(sql_group)
        group = {x[1]: x[0] for x in obj_group.fetchall()}
        result["group"] = group 

        return result



    def check_def_tables(self, values):
        current = self.get_field_options()
        # assay = values.get('assay', None)
        species = values.get("species", None)
        organ = values.get("organ", None)
        antibody = values.get("antibody", None)

        # if assay not in current.get("assay_list", []) and assay:
        #     dic = { 'assay_name': assay }
        #     self.write_row("assay_table", dic)
        
        if species not in current.get("species_list", []) and species:
            dic = { 'species_name': species }
            self.write_row("species_table", dic)

        if organ not in current.get("organ_list", []) and organ:
            dic = { 'organ_name': organ }
            self.write_row("organ_table", dic)
        
        if antibody not in current.get("antibody_list", []) and antibody:
            regulation = values.get("regulation", None)
            dic = { 'epitope': antibody, "regulation": regulation }
            self.write_row("antibody_table", dic)

    def get_field_options(self):
        conn = self.engine.connect()
        result = {}
        sql_assay = """ SELECT assay_name FROM assay_table;"""
        sql_obj_assay = conn.execute(sql_assay)
        assay_lis = self.sql_obj_to_list(sql_obj_assay)
        result["assay_list"] = assay_lis

        sql_organ = """ SELECT organ_name FROM organ_table;"""
        sql_obj_organ = conn.execute(sql_organ)
        organ_lis = self.sql_obj_to_list(sql_obj_organ)
        result["organ_list"] = organ_lis

        sql_species = """ SELECT species_name FROM species_table;"""
        sql_obj_species = conn.execute(sql_species)
        species_lis = self.sql_obj_to_list(sql_obj_species)
        result["species_list"] = species_lis

        sql_antibody = """SELECT epitope FROM antibody_table;"""
        sql_obj_antibody = conn.execute(sql_antibody)
        group_lis = self.sql_obj_to_list(sql_obj_antibody)
        result["antibody_list"] = group_lis

        sql_group = """SELECT group_name FROM groups_table;"""
        sql_obj_group = conn.execute(sql_group)
        group_lis = self.sql_obj_to_list(sql_obj_group)
        result["group_list"] = group_lis

        sql_tissue_source = """SELECT tissue_source_name FROM tissue_source_table;"""
        sql_obj_tissue_source = conn.execute(sql_tissue_source)
        tissue_source_lis = self.sql_obj_to_list(sql_obj_tissue_source)
        result["tissue_source_list"] = tissue_source_lis

        sql_publication = """SELECT pmid FROM publications;"""
        sql_obj_publication = conn.execute(sql_publication)
        publication_lis = self.sql_obj_to_list(sql_obj_publication)
        result["publication_list"] = publication_lis

        return result

    def sql_obj_display_id_list(self, sql_obj):
        print(sql_obj)
        items = sql_obj.fetchall()
        res = [{'display': x[0], 'id': x[1]} for x in items]
        return res

    def update_db_table(self, db_table, pandas_df, cols, on_col, min_id):
        conn = self.engine.connect()
        for inx, row in pandas_df.iterrows():
            on_col_value = row[on_col]

            sql = f"SELECT * FROM {db_table} WHERE {on_col} = {on_col_value};"
            sql_obj = conn.execute(sql)
            lis = self.sql_tuples_to_dict(sql_obj)
            if lis:
                #already an entry present
                db_row = lis[0]
                change_dict = {}
                for col in cols:
                    current_val = db_row[col]
                    slims_val = row[col]
                    if current_val != slims_val:
                        change_dict[col] = slims_val

                if change_dict:
                    self.edit_row(table_name=db_table, changes_dict=change_dict, on_var=on_col, on_var_value=on_col_value)

            else:
                #there is no entry present
                col_dict = self.pandas_row_to_dict(row, cols)
                self.write_row(db_table,col_dict)

    def pandas_row_to_dict(self, pandas_row, cols):
        dic = {}
        for col in cols:
            ele = pandas_row.get(col, "None")
            if ele != 'None':
                dic[col] = ele
        return dic


    def get_info_from_results_id(self, results_id):
        conn = self.engine.connect()
        sql = f"""SELECT * FROM {self.full_db_data} WHERE `results_id` = %s;"""
        tup = (results_id, )
        obj = conn.execute(sql, tup)
        result = self.sql_tuples_to_dict(obj)
        if result:
            result = result[0]
        else:
            result=["Not-Found"]
        return result

    def get_info_from_run_id(self, run_id):
        conn = self.engine.connect()
        sql = f"""SELECT * FROM {self.full_db_data} WHERE `run_id` = %s;"""
        tup = (run_id,)
        obj = conn.execute(sql, tup)
        result = self.sql_tuples_to_dict(obj)
        if not result:
            result = "Not-Found"
        return result

    def create_study(self, values_dict, result_ids):
        conn = self.engine.connect()
        self.write_row("studies", values_dict)
        sql = "SELECT MAX(study_id) FROM studies;"
        res = conn.execute(sql)
        tup = res.fetchone()
        max_id = tup[0]
        for result_id in result_ids:
            col_dict = {
                "results_id": result_id,
                "study_id": max_id
            }
            self.write_row("results_studies", col_dict)

    def get_run_ids(self):
        sql = f"""SELECT distinct run_id from {self.full_db_data} WHERE run_id IS NOT NULL;"""
        conn = self.engine.connect()
        obj = conn.execute(sql)
        res = [ {'run_id': x[0]} for x in obj.fetchall()]
        # res = self.sql_obj_to_list(obj)
        return res

    def grab_runs_homepage_group(self, group_name):
        conn = self.engine.connect()
        sql = f"SELECT * FROM {self.homepage_population_name} WHERE `group` = '{group_name}' OR public = 1;"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def grab_runs_homepage_admin(self):
        conn = self.engine.connect()
        sql = f"SELECT * FROM {self.homepage_population_name};"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def get_proper_index(self, column_name, table_name):
        conn = self.engine.connect()
        sql = f"SELECT  {column_name} FROM {table_name};"
        tuple_list = conn.execute(sql)
        index_list = [x[0] for x in tuple_list.fetchall()]
        return index_list
    
    def edit_row(self, table_name, changes_dict, on_var, on_var_value):
        conn = self.engine.connect()
        update = f"UPDATE {table_name}"
        set_sql = " SET "
        lis = []
        for key, val in changes_dict.items():
            set_sql += f"`{key}` = %s, "
            lis.append(val)
        set_sql = set_sql[:len(set_sql) - 2]
        # if isinstance(on_var_value, str):
        #     on_var_value = f"'{on_var_value}'"
        where = f" WHERE `{on_var}` = %s;"
        sql = update + set_sql + where
        lis.append(on_var_value)
        tup = tuple(lis)
        res = conn.execute(sql, tup)

    def write_row(self, table_name, values_dict):
        conn = self.engine.connect()
        INSERT = f"INSERT INTO {table_name} ("
        VALUES = ") VALUES ("
        lis = []
        for key, val in values_dict.items():
            # if isinstance(val, str):
            #     val = f"'{val}'"
            INSERT += f"{key}, "
            VALUES += "%s, "
            lis.append(val)

        INSERT = INSERT[ :len(INSERT) - 2]
        VALUES = VALUES[ :len(VALUES) - 2]
        sql = INSERT + VALUES + ");"
        tup = tuple(lis)
        conn.execute(sql, tup)

    def write_paths(self):
      filename = 'web_paths.csv'
      f = self.api_db.joinpath(filename) 
      with open(f, "r") as file:
        csv_reader = csv.reader(file, delimiter=",")
        for line in csv_reader:
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
    

    def delete_row(self, table_name, on_var, on_var_value):
        print(f"deleting: {on_var} = {on_var_value}")

    def get_epitope_to_id_dict(self):
        conn = self.engine.connect()
        sql = "SELECT epitope, antibody_id FROM antibody_table;"
        sql_obj = conn.execute(sql)
        tuple_list = sql_obj.fetchall()
        antibody_dict = {x[0]: x[1] for x in tuple_list}
        return antibody_dict

    def get_run(self, results_id, groups):
        conn = self.engine.connect()
        sql = f"SELECT * FROM {self.homepage_population_name} WHERE results_id = '{results_id}';"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        item = res[0]
        group = item['group']
        public = item['public']
        if public or group in groups or 'admin' in groups or 'user' in groups:
            return item
        return ["NOT AUTHORIZED"]
        

    def pull_view(self, view_name):
        conn = self.engine.connect()
        sql = f"SELECT * FROM {view_name};"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def sql_tuples_to_dict(self, sql_obj):
        result = []
        for v in sql_obj:
            dic = {}
            for col, val in v.items():
                dic[col] = val
            result.append(dic)
        return result 

    def sql_obj_to_list(self, sql_obj):
        res = sql_obj.fetchall()
        lis = [x[0] for x in res]
        return lis
        

    def get_paths_admin(self):
        conn = self.engine.connect()
        sql = f"SELECT results_folder_path FROM {self.homepage_population_name};"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def get_paths_group(self, group):
        conn = self.engine.connect()
        SELECT = f"SELECT results_folder_path FROM {self.homepage_population_name}"
        WHERE = f"WHERE `group` = {group} or `public` = 1;"
        sql = SELECT + WHERE
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def search_table(self, table_name, on_var, query):
        conn = self.engine.connect()
        SELECT = f"SELECT * FROM {table_name}"
        WHERE  = f" WHERE {on_var} LIKE '%%{query}%%';"
        sql = SELECT + WHERE
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

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
        conn = self.engine.connect()
        engine_result = conn.execute(sql)
        result = engine_result.fetchall()
        result_final = result[0]
        result_dict = self.list_to_dict(result_final, columns)
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

    def use_definition_table(self, old_table_name, lookup_table_name, old_column, new_column_lookup_table, id_column):
        conn = self.engine.connect()
        sql_unique = f"""SELECT distinct {old_column} FROM {old_table_name} WHERE {old_column} IS NOT NULL;"""
        res = conn.execute(sql_unique)
        vals = self.sql_obj_to_list(res)
        for val in vals:
            sql_popuale_new_table = f"""INSERT INTO {lookup_table_name} (`{new_column_lookup_table}`) VALUES ('{val}');"""
            conn.execute(sql_popuale_new_table)
        
        sql_get_mapping = f"""SELECT * FROM {lookup_table_name};"""
        result = conn.execute(sql_get_mapping)
        mapping = self.sql_tuples_to_dict(result)
        for element in mapping:
            id = element[id_column]
            name = element[new_column_lookup_table]
            sql = f"""UPDATE {old_table_name} SET {old_table_name}.{id_column} = {id} WHERE {old_table_name}.{old_column} = '{name}';"""
            conn.execute(sql)


    def createPublicTables(self, antibody_dict):
        filename = "Adatabase.xlsx"
        f = self.api_db.joinpath(filename)
        df_dict = pd.read_excel(open(f, 'rb'), sheet_name=None)
        df_publication = df_dict["Publication"]
        df_run = df_dict["Run"]
        df_authors = df_dict['authors']
        df_author_publications = df_dict["author_publications"]

        runs_df_dict = self.create_public_table_runs(df_run, antibody_dict)
        runs_df_dict["publications_public"] = df_publication
        runs_df_dict["authors_public"] = df_authors
        runs_df_dict["author_publications_join_public"] = df_author_publications

        return runs_df_dict

        # publications_df_dict = self.create_public_table_publications(df_publication, df_authors, df_author_publications)

    def create_public_table_runs(self, df_run, antibody_dict):
        tissue_slide_df = df_run.copy()
        tissue_cols = ["species", "tissue_type", "organ", "experimental_condition"]
        tissue_slide_df = tissue_slide_df[tissue_cols]
        sql = "SELECT MAX(tissue_id) FROM tissue_slides;"
        res = self.connection.execute(sql)
        tup = res.fetchone()
        max_id = tup[0]
        # tissue_slide_df.to_sql("tissue_slides", self.engine, index=False, if_exists="append")

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
        # metadata_df.to_sql("results_metadata", self.engine, index=False, if_exists="append")
        return {"metadata_results_df_public": metadata_df, "tissue_slides_df_public": tissue_slide_df}

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
        content = content.astype({"cntn_fk_contentType": str, "cntn_fk_status": str, "cntn_cf_fk_chipB": str, "pk": str})
        copy = content[(content.cntn_fk_contentType == '42') & (content.cntn_cf_runId.notnull()) & (content.cntn_cf_runId != "None") & (content.cntn_fk_status != '54') & (content.cntn_cf_fk_workflow.isin(["4","7","10"]))]
        tissue_slides = copy.copy()

        tissue_chipb = pd.merge(left = tissue_slides, right = content, how="left" ,left_on="cntn_cf_fk_chipB", right_on="pk", suffixes=("", "_bchip"))

        cols = ["cntn_cf_source","cntn_cf_roiChannelWidthUm_bchip", "cntn_cf_fk_tissueType", "cntn_cf_fk_species","cntn_cf_sampleId","cntn_cf_fk_organ","cntn_cf_experimentalCondition","cntn_cf_runId", "cntn_cf_fk_workflow", "cntn_cf_fk_epitope", "cntn_cf_fk_regulation"]
        temp_copy = tissue_chipb[cols]
        tissue_slides = temp_copy.copy()

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
        return df
        # self.write_df(df=df, table_name="antibodies")


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
        run_metadata = run_metadata.assign(web_object_available=col, public = False, group = "AtlasXomics", publication_id = pd.NA, results_source = "AtlasXomics")
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
        conn = self.engine.connect()
        sql = "DELETE FROM " + table_name + ";"
        conn.execute(sql)
        df.to_sql(table_name, self.engine, index=False, if_exists="append")

