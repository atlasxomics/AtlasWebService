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
import re

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
        self.engine = self.initialize()
        self.initEndpoints()
        self.path_db = Path(self.auth.app.config["DBPOPULATION_DIRECTORY"])
        self.bucket_name = self.auth.app.config['S3_BUCKET_NAME']
        self.aws_s3 = boto3.client('s3')
        self.homepage_population_name = "homepage_population"
        self.full_db_data = "metadata_full_db"
        self.run_job_view = "run_id_job"

    def initialize(self):
        """Initialization function for class.
        Creates and return an engine connected to the mysql database if successful.
        Returns:
            Engine: SQL Alchemy/PyMySql Engine
        """
        try:
            connection_string = "mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}".format(username=self.username, password=self.password, host=self.host, port=str(self.port), dbname=self.db)
            engine = db.create_engine(connection_string)
            return engine
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")
            return None

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
        
        @self.auth.app.route("/api/v1/run_db/populate_homepage", methods=["GET"])
        @self.auth.login_required
        def _populate_homepage():
            """Endpoint for populating landing page of atlasxplore.
            No arguments are needed to be passed to this endpoint. Instead the groups a user is a part of, based on their token, are used to determine which runs to display.

            Args: None

            Returns:
                Flask Response: Response object containing list of dictionaries with data
                about each of the runs used to populate the homepage.
                
                
            Example:
            
            [{"results_id": 1, "public": 0, "group": "AtlasXomics", "channel_width": 25, "experimental_condition" ...... }, {...}, ...]
            Where each entry in the lust is a dictionary with data about a run, coming from the `homepage_population` view.
                
            """
            sc = 200
            try:
                user, groups= current_user
                if 'admin' in groups:
                    res = self.grab_runs_homepage_admin()
                else:
                    res = self.grab_runs_homepage_groups(groups)
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
            """
            Endpoint for getting information about a run, when only the results_id is known. 
            Result_id must be referencing a run that is within the `homepage_population` view.
            **Generally we have moved away from using result_ids to identify runs, and instead use the run_id.**
            
            Required:
            Body: {
                "results_id": int
            }
            
            Returns: Flask Response Object: Response object containing dictionary with data about the run.
            
            Example:
            
            [{"results_id": 1, "public": 0, "group": "AtlasXomics", "channel_width": 25, "experimental_condition" ...... }]
            
            """
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

        @self.auth.app.route("/api/v1/run_db/search_authors", methods=["POST"])
        @self.auth.login_required
        def _search_authors():
            """API endpoint used to search the author_search table.
                Takes in `query` parameter from body of request.
                
               Required Body:
               {
                   "query": str: query to search for
               } 
                
                
            Returns:
                Flask Response Object: Response object containing result_ids for runs matching the query.
                
            Example:
            
            {
                "query": "Rong"
            }
            
            return: [{"author_name": "Rong", "results_id": 1}, {"author_name": "Rong", "results_id": 2}, ...]
                
            """
            params = request.get_json()
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
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_field_options", methods=['GET'])
        @self.auth.login_required
        def _get_field_options():
            """API Endpoint used to obtain the options available for dropdowns used in AtlasWeb.
            Takes in no arguments, instead uses the groups a user is a part of to determine which options to return.
            
            The only dropdown option that is dependent on the user's group is the `group` dropdown.
            
            Other dropdowns options returned are:
            
            assay_list: list of assays available in the assay_table
            organ_list: list of organs available in the organ_table
            species_list: list of species available in the species_table
            antibody_list: list of antibodies available in the antibody_table
            tissue_source_list: list of tissue sources available in the tissue_source_table
            publication_list: list of publications available in the table publications
            
            

            Returns:
                Flask Response: Dictionary with keys corresponding to the dropdowns, and values being lists of options for each dropdown.
                
            Example:
                Non Admin
                {"assay_list": ["ATAC-seq", "Transcriptome", "CUT&Tag"], 
                "organ_list": ["embryo", "brain", "kidney", "polyp", "liver"],
                "species_list": ["mus_musculus", "homo_sapiens", "rattus_norvegicus"],
                "antibody_list": ["H3K27me3", "H3K27ac","H3K4me3"], 
                "tissue_source_list": ["Zyagen", "NCI"],
                "publication_list": [35978191, 36604544],
                "tissue_type_list": ["fresh_frozen", "ffpe", "efpe"] }
            """
            sc = 200
            try:
                user, groups = current_user
                res = self.get_field_options(groups)
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
            """API Endpoint used to create list of run ids currently available in the database.
            This endpoint queries the view: 
            
            No Arguments.
            
            Returns:
                Flask Response Object: Containing list of dictionaries with keys "run_id" and "tissue_id"
                
                
            Example:
            
            [{"run_id": 1, "tissue_id": 1}, {"run_id": 2, "tissue_id": 2}, ...]
                
            """
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
        
        @self.auth.app.route("/api/v1/run_db/get_studies", methods=['GET'])
        @self.auth.login_required
        def _get_study_ids():
            """Lists all studies in the database.

            Returns:
                Flask Response Object: Containing list of dictionaries with keys "study_id", "study_name", "study_description", "study_type_name", "study_type_id"
                
                
            Example: [{"study_id": 1, "study_name": "AtlasXomics", "study_description": "AtlasXomics", "study_type_name": "AtlasXomics", "study_type_id": 1}, {...}, ...]
                
            """
            sc = 200
            try:
                res = self.get_studies()
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                print(resp)
                return resp
            
        @self.auth.app.route("/api/v1/run_db/get_study_runs", methods=['POST'])
        @self.auth.login_required
        def _get_study_runs():
            sc = 200
            params = request.get_json()
            study_id = params["study_id"]
            try:
                res = self.get_study_runs(study_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/update_study_table", methods=['POST'])
        @self.auth.login_required
        def _update_study_table():
            sc = 200
            params = request.get_json()
            study_id = params["id"]
            adding_list = params["adding_list"]
            removing_list = params["removing_list"]
            study_description = params["description"]
            study_name = params["study_name"]
            study_type_id = params["study_type_id"]
            try:
                res = self.update_study_table(study_id, study_name, study_type_id, study_description, adding_list, removing_list)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/set_run_files", methods=['POST'])
        @self.auth.admin_required
        def _set_run_files():
            """API endpoint used for setting files to be associated with a given run.
            Body Payload Contains:
                adding_list: List of dictionaries where each entry entry contains information about a file being added.
                file_ids_to_remove: List of file ids that are going to be un-associated with a given run.
                file_changes: List of dictionaries, containg the file_id to be changed then all the key value pairs to be updated.

            Returns:
                Flask Response: Response object indicating whether the operation was successful.
            """
            sc = 200
            params = request.get_json()
            tissue_id = params.get("tissue_id", None)
            adding_list = params.get("files_to_add", [])
            removing_list = params.get("file_ids_to_remove", [])
            changes_list = params.get("file_changes", [])
            try:
                res = self.assign_run_files(tissue_id, adding_list, removing_list, changes_list)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_file_paths_for_run", methods=['POST'])
        @self.auth.login_required
        def _get_file_paths_for_run():
            sc = 200
            try:
                params = request.get_json()
                tissue_id = params.get("tissue_id", None)
                res = self.get_file_paths_for_run(tissue_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_file_info_run_id", methods=["POST"])
        @self.auth.login_required
        def _get_files_for_run():
            """Endpoint to receive a list of files associated with a given run.
            
            Args:
            Body: { "run_id": run_id}
            run_id: The run_id of the run to get files for.

            Returns:
                Flask Response Object: Response object containing a list of dictionaries each with file information.
                For each dictionary there are keys: 
                'file_id' The Primary Key of the file in the `file_tissue_table`.
                'bucket_name' The name of the S3 bucket the file is stored in.
                `tissue_id` The tissue_id of the tissue the file is associated with.
                'filename_short` The name of file not full path.
            
            
            Example:
            
            Body: { "run_id": "D00152" }
            
            Return: [{'file_id': 1, 'bucket_name': 's3://bucket_name', 'tissue_id': 1020, 'filename_short': 'file_name.txt'}, {...}, ... ]
                
            """
            sc = 200
            try:
                params = request.get_json()
                run_id = params["run_id"]
                res = self.get_file_info_from_run_id(run_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_all_downloadable_files_run_id", methods=["GET"])
        @self.auth.login_required
        def _get_all_downloadable_files_run_id():
            """ Endpoint that grabs all information about files that are downloadable for a given user, based on Cognito access permissions.
            Determines access permissions based on current_user callback function.
            
            No Args

            Returns:
                Flask Response: Dictionary response where each key is a run_id and each value is a list of dictionaries containing file information.
                Each element in the list of a given run_id is a dictionary with keys:
                
                "file_id": The Primary Key of the file in the `file_tissue_table`.
                "bucket_name": The name of the S3 bucket the file is stored in.
                "tissue_id": The tissue_id of the tissue the file is associated with.
                "filename_short": The display name of the file, not the full path.
                "file_type_id": The id of the type of the file based on the table `file_type_table`.
                "file_type_name": The name of the file type.
                "file_path": The full path of the file in the S3 bucket.
                "file_description": The description of the file.
                "run_id": The run_id of the run the file is associated with.
                "group_name": The name of the group the file is associated with.
                "group_id": The id of the group the file is associated with.
                
            Example:
            
            No Args:
            
            Returns: {"D00152": [{"file_id": 1, "bucket_name": "s3://bucket_name", "tissue_id": 1020, "filename_short": "file_name.txt", "file_type_id": 1, "file_type_name": "fastq", "file_path": "s3://bucket_name/file_name.txt", "file_description": "This is a file", "run_id": "D00152", "group_name": "group_name", "group_id": 1}, {...}, ... ], "D00153": [...], ...]}
                
            """
            sc = 200
            try:
                user, groups = current_user
                res = self.get_all_downloadable_files_run_id(groups)
                resp = Response(json.dumps(res), sc)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
                resp = Response(json.dumps(res), sc)
                print(res)
            finally:
                return resp
        
        
        @self.auth.app.route("/api/v1/run_db/get_file_type_options", methods=['GET'])
        @self.auth.login_required
        def _get_file_type_options():
            """Endpoint used to grab all of the available file types in the database. These are coming from the table `file_type_table`.

            No Args

            Returns:
                Flask Return Obj: Return object contains a list of dictionaries with keys:
                "file_type_id": The id of the file type.
                "file_type_name": The name of the file type.
                
            Example:
            
            Returns: [{"file_type_id": 1, "file_type_name": "fastq"}, {...}, ...]
            
            """
            sc = 200
            try:
                res = self.get_file_type_options()
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message("{} {}".format(str(e), exc))
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_study_types", methods=['GET'])
        @self.auth.login_required
        def _get_study_types():
            sc = 200
            try:
                res = self.get_study_types()
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
            """
            Endpoint that searches the pmids of the database.
            Result id's, being the primary keys of the results_metadata page, that are associated with the pmid are returned.
            
            Args:
            In Body:
                {"query": "pmid value"}
                
            Returns:
                Flask Response: List response, where each element is a dictionary with keys:
                    "result_id": The primary key of the result in the `results_metadata` table.
                    "pmid": The pmid matched from the query.           
            """
            params = request.get_json()
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
            print(sc)
            try:
                user, groups = current_user
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
            """
            Endpoint used to grab all relevant metadata associated with a run id.
            Information is coming from the `metadata_full_db` view.
            
            Args: 
            Body: {"run_id": run id used}
            run_id: The run id for which the metadata is being retrieved.

            Returns:
                Flask Object: Flask object containing a dictionary with keys being the columns from the metadata_full_db view.
                
            Example:
                {"run_id": "S8"}
                
                Returns: [{"tissue_id": 7965, "run_id": "S8", "tissue_source": "Janvier", "species": "mus_musculus", "organ": "embryo",
                        "tissue_type": "fresh_frozen", "sample_id": "S8", "experimental_condition": "Normal", "number_channels": null,
                        "image_folder_path": null, "results_id": 11067, "ngs_id": null, "epitope": null, "assay": "ATAC-seq", "pmid": 36604544,
                        "public": 1, "group": "Stahl", "web_object_available": 1, "date": 1679443200000, "results_folder_path":
                        "S3://atx-cloud-dev/data/S8/", "channel_width": 50, "result_title": "Spatial ATAC-seq data of mouse embryo",
                        "result_description": "Tiled view of E12.5 and E13.5 mouse embryos (two replicates each) with 55 um resolution"}
                        ]
            
            """
            
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
                print(res)
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/upload_metadata_page", methods=["POST"])
        @self.auth.admin_required
        def _upload_metadata_page():
            """
            Endpoint used to upload metadata pertaining to a particular run to the database.
            This is primarily used as the endpoint for the "Add a run Page" on the front end.

            Args:
            The body of the request should be a dictionary with keys being columns for the tables to be updated.
            These being the `tissue_slides` and `results_metadata` tables.
            Any columns that using lookup tables, such as `assay`, the readable name can just be specified and the endpoint will handle it,
            either by creating a new row in the lookup table or using an existing one.
            
            Not all possible columns need to be specified in the body, only the ones that are being updated.
            
            Body: {
                "run_id": "S8",
                "assay": "ATAC-seq",
                "organ": "embryo",
                ...
            }
            
            Returns:
                Flask Object: Flask object containing either a `Success` string or an error message.
                
                
            Example:
            
            {
                "assay": "ATAC-seq",
                "species": "Mouse",
                "organ": "",
                "run_id": "S0",
                "run_description": "Study S0",
                "run_title": "Study S0",
                "web_obj_path": "S3://atx-cloud-dev/study/S0/",
                "epitope": "",
                "regulation": "",
                "tissue_source": "",
                "sample_id": "",
                "experimental_condition": "",
                "pmid": "",
                "group": "AtlasXomics",
                "public": false,
                "date": null,
                "channel_width": null,
                "number_channels": null,
                "ngs_id": "",
                "results_id": null
            }
            
            Returns: "Success"
                
            """
            sc = 200
            values = request.get_json()
            try:
                user, groups = current_user
                self.check_def_tables(values, groups)
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
                return resp

    

        @self.auth.app.route("/api/v1/run_db/create_reference_table", methods=["POST"])
        @self.auth.admin_required
        def _create_reference_table():
            sc = 200
            params = request.get_json()
            old_table_name = params["old_table_name"]
            ref_table = params["ref_table"]
            old_col = params["old_column"]
            new_column_ref_table = params["new_column_ref_table"]
            id_column = params["id_column"]
            try:
                self.create_reference_table(old_table_name, ref_table, old_col, new_column_ref_table, id_column)
                res = "Success"
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{str(e)} {exc}", sc)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
        @self.auth.app.route("/api/v1/run_db/get_jobs", methods=["POST"])
        @self.auth.login_required
        def _get_jobs():
            """Endpoint used to retrieve information about jobs based on critieria specified by the user.
            
            Possible filters of jobs are by username, job_name, and run_id.
            
            Args:
            
            Body: { "filter_username": bool, "job_name": str, "run_id": str }
            
            filter_username: If True, only jobs that the user has run will be returned.
            job_name: If specified, only jobs with this name will be returned.
            run_id: If specified, only jobs that were run on this run_id will be returned.

            Returns:
                Flask Return Obj: List of dictionaries containing information about the jobs.
                Each dictionary contains the following keys:
                "job_id": int - The id of the job in the database.
                "job_name": str - The name of the job.
                "job_status": str - The status of the job (SUCCESS, INPROGRESS, FAILED).
                "job_description": str - The description of the job.
                "job_start_time": str - The time the job started.
                "job_completion_time": str - The time the job completed.
                "username": str - The username of the user who ran the job.
                "job_execution_time": str - The time it took for the job to complete.
                "run_id": str - The run_id that the job was run on.
                "tissue_id": str - The tissue_id that the job was run on.
                
                
            Example:
            
            Body: { "job_name": "test_job", "run_id": "S0", filter_username: "false" }
            
            Return: [ {"job_id": 1, "job_name": "test_job" "job_status": "FAILED", "job_description": "description", "job_start_time":
            "2023-02-17 10:14:13", "job_completion_time": null, "username": "admin", "job_execution_time": null, "run_id": "S0",
            "tissue_id": 500}, {...}, {...}, ...]
            
                
            """
            sc = 200
            data = request.get_json()
            filter_username = bool(data.get("filter_username", False))
            job_name = data.get("job_name", None)
            run_id = data.get("run_id", None)
            try:
                if filter_username:
                    username = self.get_username()
                else:
                    username = None
                res = self.get_jobs(username, job_name, run_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp

        @self.auth.app.route("/api/v1/run_db/ensure_run_id_created", methods=["POST"])
        @self.auth.login_required
        def _ensure_run_id_created():
            """
            Endpoint used to ensure that a run_id has been created in the database.
            If the run_id does not exist, it is added.
            
            Args: Body: { "run_id": str }
            
            run_id: Id of the run that is having is existence confirmed.
            
            Returns: Flask Return Obj containing a `Success` message if a run_id is confirmed, and an error message if endpoint failed.            
            """ 
            sc = 200
            data = request.get_json()
            run_id = data.get("run_id", None)
            try:
                res = self.ensure_run_id_created(run_id)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
            

            
        @self.auth.app.route("/api/v1/run_db/insert_new_publication", methods=["POST"])
        @self.auth.login_required
        def _insert_new_publication():
            sc = 200
            data = request.get_json()
            try:
                pmid = data['pmid']
                title = data['title']
                link = data['link']
                date = data['date']
                author_names = data['author_names']
                journal = data['journal']
                res = self.insert_pmid_author(pmid, title, link, date, author_names, journal)
            except Exception as e:
                sc = 500
                exc = traceback.format_exc()
                res = utils.error_message(f"{e} {exc}")
                print(res)
            finally:
                resp = Response(json.dumps(res), sc)
                return resp
        
    def insert_pmid_author(self, pmid, title, link, date, author_names, journal):
        if not pmid:
            raise Exception("PMID not found")
        conn = self.get_connection()
        insert_pub_tmpl = f"""INSERT INTO publications (pmid, journal, date, title, link) VALUES (%s, %s, %s, %s, %s)"""
        conn.execute(insert_pub_tmpl, (pmid, journal, date, title, link,))
        get_pub_id_tmpl = f"""SELECT publication_id FROM publications WHERE pmid=%s"""
        pub_id  = conn.execute(get_pub_id_tmpl, (pmid)).fetchall()[0][0]
        if type(author_names) == list:
            auth_names_str  = "('" + "'), ('".join(author_names) + "')"
            search_auth_tmpl = f"""
            WITH t (name) AS (VALUES {auth_names_str}) 
            SELECT name FROM t 
            LEFT OUTER JOIN authors on authors.author_name=t.name
            WHERE authors.author_name IS null
            """
            sql_obj = conn.execute(search_auth_tmpl).fetchall()
            if sql_obj:
                sql_obj = [str(i).replace(",","") for i in sql_obj]
                insert_auth_str = ','.join(str(x) for x in sql_obj)
                insert_auth_tmpl = f"""INSERT INTO authors (author_name) VALUES {insert_auth_str}"""
                conn.execute(insert_auth_tmpl)
            author_ids = []
            for a in author_names:
                rslt = self.search_table("authors", "author_name", a)
                rslt = rslt[0]['author_id']
                author_ids.append(rslt)
            tupl = [(pub_id, i) for i in author_ids]
            tupl = str(tupl).replace("[", "").replace("]","")
            insert_auth_pub_tmpl = f"""INSERT INTO author_publications (publication_id, author_id) VALUES {tupl}"""
            conn.execute(insert_auth_pub_tmpl)
        return "Success"

    def get_connection(self):
        return self.engine.connect()
    
    def get_username(self):
        user, groups = current_user
        return user.username
    
    def ensure_run_id_created(self, run_id):
        sql_check_existence = f"""SELECT * FROM tissue_slides WHERE run_id = %s"""
        conn = self.get_connection()
        res = conn.execute(sql_check_existence, (run_id, ))
        if res.rowcount == 0:
            sql_insert = f"""INSERT INTO tissue_slides (run_id) VALUES (%s)"""
            conn.execute(sql_insert, (run_id, ))
        sql_tissue_id = f"""SELECT tissue_id FROM tissue_slides WHERE run_id = %s"""
        res = conn.execute(sql_tissue_id, (run_id, ))
        tissue_id = res.fetchone()[0]
        check_result_existence = f"""SELECT * FROM results_metadata WHERE tissue_id = %s"""
        res = conn.execute(check_result_existence, (tissue_id, ))
        if res.rowcount == 0:
            sql_insert_tissue_id = f"""INSERT INTO results_metadata (tissue_id) VALUES (%s)"""
            conn.execute(sql_insert_tissue_id, (tissue_id, ))
        return "Success"

    def get_jobs(self, username, job_name, run_id):
        sql, arg_lis = self.generate_get_jobs_sql(username, job_name, run_id)
        conn = self.get_connection()
        if arg_lis:
            sql_obj = conn.execute(sql, arg_lis)
        else:
            sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        for r in res:
            if r["job_start_time"]:
                r["job_start_time"] = datetime.datetime.fromtimestamp(int(r["job_start_time"] // 1000)).strftime("%Y-%m-%d %H:%M:%S")
            if r["job_completion_time"]:
                r["job_completion_time"] = datetime.datetime.fromtimestamp(int(r["job_completion_time"] // 1000)).strftime("%Y-%m-%d %H:%M:%S")
        return res
    
    
    def generate_get_jobs_sql(self, username = None, job_name = None, run_id = None):
        arg_lis = []
        variables = [username, job_name, run_id]
        select_sql = f"""SELECT * FROM {self.run_job_view} """
        where_sql = """WHERE """
        for i, v in enumerate(variables):
            if v:
                if i == 0:
                    where_sql += f"""username = %s """
                    arg_lis.append(v)
                elif i == 1:
                    if username:
                        where_sql += """AND """
                    where_sql += f"""job_name = %s """
                    arg_lis.append(v)
                elif i == 2:
                    if username or job_name:
                        where_sql += """AND """
                    where_sql += f"""run_id = %s """
                    arg_lis.append(v)
        sql = select_sql + where_sql
        return sql, tuple(arg_lis)

    def get_job_info(self, run_id, job_name):
        conn = self.get_connection()
        sql = f"""SELECT * FROM {self.run_job_view} WHERE run_id = %s AND job_name = %s AND job_start_time = 
        (SELECT MAX(job_start_time) FROM {self.run_job_view} WHERE run_id = %s AND job_name = %s)"""
        sql_obj = conn.execute(sql, (run_id, job_name, run_id, job_name))
        res = self.sql_tuples_to_dict(sql_obj)
        if res:
            res = res[0]
        return res

    def grab_summary_stats(self, group):
        conn = self.get_connection()
        sql = f"""SELECT assay as variable, count(assay) as count FROM {self.homepage_population_name} WHERE (`group` = %s OR public = 1) group by assay
                    UNION SELECT `group` as variable, count(`group`) as count FROM {self.homepage_population_name} WHERE (`group` = %s OR public = 1) group by `group`"""
        tup = (group, group)
        sql_obj = conn.execute(sql, tup)
        res = sql_obj.fetchall()
        result = {x[0]: x[1] for x in res}
        return result

    def grab_summary_stat_admin(self):
        conn = self.get_connection()
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
        tissue_type_id = mapping_dict["tissue_type"].get(tissue_type, None)

        sample_id = values.get("sample_id", None)
        experimental_condition = values.get("experimental_condition", None)
        channel_width = values.get("channel_width", None)
        if not channel_width:
            channel_width = None
        number_channels = values.get("number_channels", None)
        if not number_channels:
            number_channels = None

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
            "channel_width": channel_width,
            "number_channels": number_channels,
            "tissue_type_id": tissue_type_id
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
            "result_date": result_date
        }
        #check if run_id is present in tissue_slides
        conn = self.get_connection()
        sql_check_existence = f"""SELECT tissue_id FROM tissue_slides WHERE run_id = %s;"""
        tup = (run_id, )
        obj = conn.execute(sql_check_existence, tup)
        ele = obj.fetchone()
        
        if ele:
            tissue_dict_set = set(tissue_dict.keys())
            self.edit_row("tissue_slides", tissue_dict, "run_id", run_id, tissue_dict_set)
        else:
            self.write_row("tissue_slides", tissue_dict)
        if results_id:
            results_dict_set = set(result_dict.keys())
            self.edit_row("results_metadata", result_dict, "results_id", results_id, results_dict_set)
        else:
            conn = self.get_connection()
            sql_get_tissue_id = """SELECT tissue_id FROM tissue_slides WHERE run_id = %s;"""
            tup = (run_id,)
            sql_obj = conn.execute(sql_get_tissue_id, tup)
            id = sql_obj.fetchone()[0]
            result_dict['tissue_id'] = id
            self.write_row("results_metadata", result_dict)

    def get_def_table_mappings(self):
        result = {}
        conn = self.get_connection()
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

        sql_tissue_type = """SELECT * FROM tissue_type_table;"""
        obj_tissue_type = conn.execute(sql_tissue_type)
        tissue_type = {x[1]: x[0] for x in obj_tissue_type.fetchall()}
        result["tissue_type"] = tissue_type 

        return result



    def check_def_tables(self, values, groups):
        """Method to ensure that the values being entered into database exist in proper lookup tables.
        If a value for a given column is not currently within the designated lookup table, it is written.
        This only relates to the fields in which a user is able to specify a new, custom field.
        
        Args:
            values dict: key: column name value: column value. Used for passing in values being written.
            groups (_type_): Groups the current user belongs to. 
        """
        current = self.get_field_options(groups)
        species = values.get("species", None)
        organ = values.get("organ", None)
        antibody = values.get("epitope", None)
        tissue_source = values.get("tissue_source", None)
        tissue_type = values.get("tissue_type", None)

        if species not in current.get("species_list", []) and species:
            dic = { 'species_name': species }
            self.write_row("species_table", dic)

        if organ not in current.get("organ_list", []) and organ:
            dic = { 'organ_name': organ }
            self.write_row("organ_table", dic)
        
        if antibody not in current.get("antibody_list", []) and antibody:
            dic = { 'epitope': antibody }
            self.write_row("antibody_table", dic)
        
        if tissue_source not in current.get("tissue_source_list", []) and tissue_source:
            dic = { "tissue_source_name": tissue_source }
            self.write_row("tissue_source_table", dic)
        
        if tissue_type not in current.get("tissue_type_list", []) and tissue_type:
            dic = { "tissue_type_name": tissue_type }
            self.write_row("tissue_type_table", dic)

    def get_field_options(self, groups):
        """
        Method used to obtain available options for dropdowns.
        This method queries lookup tables in the database and converts the result into a dictionary of lists.

        Returns Dict: key: Name of dropdown list Value: List of available options for a given dropdown list.
        """
        
        conn = self.get_connection()
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


        sql_tissue_source = """SELECT tissue_source_name FROM tissue_source_table;"""
        sql_obj_tissue_source = conn.execute(sql_tissue_source)
        tissue_source_lis = self.sql_obj_to_list(sql_obj_tissue_source)
        result["tissue_source_list"] = tissue_source_lis

        sql_publication = """SELECT pmid FROM publications;"""
        sql_obj_publication = conn.execute(sql_publication)
        publication_lis = self.sql_obj_to_list(sql_obj_publication)
        result["publication_list"] = publication_lis

        sql_tissue_type = """SELECT tissue_type_name from tissue_type_table;"""
        sql_obj_tissue_type = conn.execute(sql_tissue_type)
        tissue_type_list = self.sql_obj_to_list(sql_obj_tissue_type)
        result["tissue_type_list"] = tissue_type_list

        # Only add a groups list entry if user is an admin
        if 'admin' in groups:
            sql_group = """SELECT group_name FROM groups_table;"""
            sql_obj_group = conn.execute(sql_group)
            group_lis = self.sql_obj_to_list(sql_obj_group)
            result["group_list"] = group_lis

        return result


    def get_info_from_results_id(self, results_id):
        conn = self.get_connection()
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
        conn = self.get_connection()
        sql = f"""SELECT * FROM {self.full_db_data} WHERE `run_id` = %s;"""

        tup = (run_id,)
        obj = conn.execute(sql, tup)
        result = self.sql_tuples_to_dict(obj)
        if not result:
            result = ["Not-Found"]
        return result

    def get_file_info_from_run_id(self, run_id):
        conn = self.get_connection()
        sql = """SELECT * FROM files_run_id_view where run_id = %s"""
        obj = conn.execute(sql, (run_id, ))
        result = self.sql_tuples_to_dict(obj)
        return result
    
    def get_run_ids(self):
        """
        Method that creates a list of all available run_ids and their corresponding tissue_id.
        This is coming from the view full_db_data, which will have every run_id present in tissue_slides, 
        not just ones that have a web_object already created.
        """
        sql = f"""SELECT run_id, tissue_id from {self.full_db_data} WHERE run_id IS NOT NULL;"""
        conn = self.get_connection()
        obj = conn.execute(sql)
        lis_dic = self.sql_tuples_to_dict(obj)
        return lis_dic 

    def get_tissue_id_from_run_id(self, run_id):
        sql = """SELECT tissue_id from tissue_slides WHERE run_id = %s;"""
        conn = self.get_connection()
        obj = conn.execute(sql, (run_id,))
        tissue_id = obj.fetchone()

        if tissue_id:
            return tissue_id[0]
        else:
            return None
    
    def get_file_paths_for_run(self, tissue_id):
        sql = """SELECT * FROM files_tissue_table WHERE tissue_id = %s;"""
        conn = self.get_connection()
        obj = conn.execute(sql, (tissue_id,))
        res = self.sql_tuples_to_dict(obj)
        return res 
    
    def assign_run_files(self, tissue_id, adding_lis, removing_lis, changes_list):
        """Method used to funnel changes about files to appropriate methods.

        Args:
            tissue_id int: PK of run_id that has these files associated with it.
            adding_lis (List): List of files being added and associated to run
            removing_lis (List): List of file_ids being un-associated
            changes_list (List): List of dictionaries of changes to files.

        Returns:
            string: String indicating success.
        """
        for file_id in removing_lis:
            self.remove_file_from_run(file_id)
        
        for file_obj in adding_lis:
            self.add_file_to_run(tissue_id, file_obj)
            
        for file_obj in changes_list:
            self.edit_file_from_run(file_obj)
        return 'Success'
    
    
    def edit_file_from_run(self, file_obj):
        key_set = set({'file_path', 'file_type_id', 'tissue_id', 'file_description', 'bucket_name', 'filename_short'})
        if 'file_type_id' in file_obj.keys() and not file_obj['file_type_id']:
            file_obj['file_type_id'] = self.add_file_type(file_obj['file_type_name'])
        if "file_path" in file_obj.keys():
            file_obj["filename_short"] = self.get_filename_from_S3_path(file_obj['file_path'])
        self.edit_row('files_tissue_table', file_obj, 'file_id', file_obj['file_id'], key_set)
    
    def get_filename_from_S3_path(self, s3_path):
        filename = re.split("/", s3_path)[-1]
        return filename
    
    def remove_file_from_run(self, file_id):
        """Method for removing a file from the files_tissue_table table, based on file_id.

        Args:
            file_id int: PK of the file to be removed.

        Raises:
            Exception: If the file_id is null.
        """
        conn = self.get_connection()
        if not file_id:
            raise Exception('file_id not found')
        sql = "DELETE FROM files_tissue_table WHERE file_id = %s"
        conn.execute(sql, (file_id,))
    
    def get_file_type_options(self):
        sql = "SELECT file_type_id, file_type_name FROM file_type_table;"
        conn = self.get_connection()
        obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(obj)
        return res
    
    def get_all_downloadable_files_run_id(self, groups):
        sql = "SELECT * from files_run_id_view"
        lis = []
        if "admin" not in groups:
            where = " WHERE `group_name` in ("
            for group in groups:
                lis.append(group)
                where += f"%s, "
            where = where[:-2] + ")"
            sql = sql + where
        
        t = tuple(lis)
        conn = self.get_connection()
        res = conn.execute(sql, t)
        result = self.sql_tuples_to_dict(res)
        
        run_mapping = {}
        for file in result:
            run_id = file["run_id"]
            current_list = run_mapping.get(run_id, [])
            file["presigned_url"] = None
            current_list.append(file)
            run_mapping[run_id] = current_list
        return run_mapping
        
            
    
    def add_file_to_run(self, tissue_id, file_obj):
        if not tissue_id:
            raise Exception('tissue_id not found')
        
        # ensure there is a value for file_path variable
        file_path = file_obj.get('file_path', None)
        if not file_path:
            raise Exception('file_path not found')
        
        bucket_name = file_obj.get("bucket_name", None)
        if not bucket_name:
            raise Exception("bucket name not found")
        
        #checking if the value is already present in db
        file_type_id = file_obj.get('file_type_id', None)
        if not file_type_id:
            # if not there, use name to create one, if no name, raise exception
            file_type_name = file_obj.get('file_type_name', None)
            if not file_type_name:
                raise Exception('file_type_name not found')
            file_type_id = self.add_file_type(file_type_name)
        
        #creating filename from file_path
        split_lis = file_path.split("/")
        filename = split_lis[-1]
        
        # grab the file description and insert all values into db 
        description = file_obj.get('file_description', None)
        sql = """INSERT INTO files_tissue_table (tissue_id, file_type_id, file_path, file_description, bucket_name, filename_short) VALUES (%s, %s, %s, %s, %s, %s);"""
        conn = self.get_connection()
        conn.execute(sql, (tissue_id, file_type_id, file_path, description, bucket_name, filename))
    
    def add_file_type(self, file_type_name):
        conn = self.get_connection()
        sql = """INSERT INTO file_type_table (file_type_name) VALUES (%s);"""
        res = conn.execute(sql, (file_type_name,))
        return res.lastrowid
         
    def get_study_types(self):
        sql = """SELECT study_type_name, study_type_id from study_type_table;"""
        conn = self.get_connection()
        obj = conn.execute(sql)
        lis = self.sql_tuples_to_dict(obj)
        return lis
    
    def get_study_runs(self, study_id):
        conn = self.get_connection()
        sql = """SELECT run_id, tissue_id FROM study_run_id WHERE study_id = %s;"""
        res = conn.execute(sql, (study_id,))
        dic_lis = self.sql_tuples_to_dict(res)
        return dic_lis
        
    def get_studies(self):
        sql = f"""SELECT study_name, study_id, study_description, study_type_name, study_type_id from study_view;"""
        conn = self.get_connection()
        res = conn.execute(sql)
        lis = self.sql_tuples_to_dict(res)

        return lis
    def update_study_table(self, study_id, study_name, study_type_id, study_description, adding_list, removing_list):
        # check if study exists
        if not study_id:
            study_id = self.create_study(study_name, study_type_id, study_description)
        else:
            self.add_study_description(study_id, study_description)
            self.add_study_type(study_id, study_type_id)
        for item in removing_list:
            tissue_id = item["tissue_id"]
            self.remove_study_run(study_id, tissue_id)
        for item in adding_list:
            tissue_id = item["tissue_id"]
            self.add_study_run(study_id, tissue_id)
        return "Success"
    
    def add_study_description(self, study_id, study_description):
        conn = self.get_connection()
        sql = """UPDATE study_table SET study_description = %s WHERE study_id = %s;"""
        conn.execute(sql, (study_description, study_id))
    
    def add_study_type(self, study_id, study_type_id):
        conn = self.get_connection()
        sql = """UPDATE study_table SET study_type_id = %s WHERE study_id = %s;"""
        conn.execute(sql, (study_type_id, study_id))
    
    def create_study(self, study_name, study_type_id, study_description):
        conn = self.get_connection()
        sql = """INSERT INTO study_table (study_name, study_type_id, study_description) VALUES (%s, %s, %s);"""
        res = conn.execute(sql, (study_name, study_type_id, study_description))
        study_id = res.lastrowid
        return study_id

    def get_study_id_from_name(self, study_name):
        conn = self.get_connection()
        sql = f"""SELECT study_id FROM study_table WHERE study_name = %s;"""
        res = conn.execute(sql, (study_name,))
        res = res.fetchone()
        if res:
            return res[0]
        else:
            return None
    
    def remove_study_run(self, study_id, tissue_id):
        conn = self.get_connection()
        sql = f"""DELETE FROM study_tissue_table WHERE study_id = %s AND tissue_id = %s;"""
        conn.execute(sql, (study_id, tissue_id))
    
    def add_study_run(self, study_id, tissue_id):
        conn = self.get_connection()
        sql = """INSERT INTO study_tissue_table (study_id, tissue_id) VALUES (%s, %s);"""
        conn.execute(sql, (study_id, tissue_id))
    
    def grab_runs_homepage_groups(self, groups):
        """Method to access homepage_population view and only retrieve
        runs corresponding to a particular set of groups.

        Args:
            groups list: list of group names

        Returns:
            _type_: _description_
        """
        tup, sql = self.grab_runs_homepage_groups_sql(groups)
        conn = self.get_connection()
        sql_obj = conn.execute(sql, tup)
        res = self.sql_tuples_to_dict(sql_obj)
        return res
    
    def grab_runs_homepage_groups_sql(self, groups):
        """Generates string of sql query based on a list of group names provided.

        Args:
            groups List: List of group names to be included in query 

        Returns:
            tuple: 0: tuple containting each of the groups provided in the groups arg
                    1: string of the sql statement to be used. 
        """
        tup = tuple(groups)
        sql = f"SELECT * FROM {self.homepage_population_name} WHERE "
        in_sql = ""
        if groups:
            in_sql = "`group` IN ("
            for i in range(len(groups)):
                in_sql += "%s, "
            in_sql = in_sql[:-2] + ") OR "
        groups = "public = 1;"
        sql = sql + in_sql + groups
        return (tup, sql)

    def grab_runs_homepage_admin(self):
        """Selects all runs from homepage_population view.
        Returns:
            List[dictionary]: List of dictionary with each entry in list being a run.
        """
        conn = self.get_connection()
        sql = f"SELECT * FROM {self.homepage_population_name};"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def edit_row(self, table_name, changes_dict, on_var, on_var_value, key_set=None):
        if not key_set:
            return
        conn = self.get_connection()
        update = f"UPDATE {table_name}"
        set_sql = " SET "
        lis = []
        for key, val in changes_dict.items():
            if key not in key_set:
                continue
            set_sql += f"`{key}` = %s, "
            lis.append(val)
        set_sql = set_sql[:len(set_sql) - 2]
        
        where = f" WHERE `{on_var}` = %s;"
        sql = update + set_sql + where
        lis.append(on_var_value)
        tup = tuple(lis)
        conn.execute(sql, tup)

    def write_row(self, table_name, values_dict):
        conn = self.get_connection()
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

    

    def delete_row(self, table_name, on_var, on_var_value):
        print(f"deleting: {on_var} = {on_var_value}")

    def get_epitope_to_id_dict(self):
        conn = self.get_connection()
        sql = "SELECT epitope, antibody_id FROM antibody_table;"
        sql_obj = conn.execute(sql)
        tuple_list = sql_obj.fetchall()
        antibody_dict = {x[0]: x[1] for x in tuple_list}
        return antibody_dict

    def get_run(self, results_id, groups):
        conn = self.get_connection()
        sql = f"SELECT * FROM {self.homepage_population_name} WHERE results_id = %s;"
        tup = (results_id, )
        sql_obj = conn.execute(sql, tup)
        res = self.sql_tuples_to_dict(sql_obj)
        item = res[0]
        group = item['group']
        public = item['public']
        if public or group in groups or 'admin' in groups or 'user' in groups:
            return item
        return ["NOT AUTHORIZED"]
        

    def sql_tuples_to_dict(self, sql_obj):
        """Helper method used to convert the sqlalchemy Cursor Result return object into JSON serializeable list of dicionaries

        Args:
            sql_obj CursorResult: Result of cursor execution of sql statement. 

        Returns:
            List[dictionary]: List where each element is a dictionary. Key: column name Value: value
        """
        result = []
        for v in sql_obj:
            d = dict(v._mapping)
            result.append(d)

        return result 

    def sql_obj_to_list(self, sql_obj):
        res = sql_obj.fetchall()
        lis = [x[0] for x in res]
        return lis
        

    def get_paths_admin(self):
        conn = self.get_connection()
        sql = f"SELECT results_folder_path FROM {self.homepage_population_name};"
        sql_obj = conn.execute(sql)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def get_paths_group(self, group):
        conn = self.get_connection()
        SELECT = f"SELECT results_folder_path FROM {self.homepage_population_name}"
        WHERE = f"WHERE `group` = %s or `public` = 1;"
        tup = (group, )
        sql = SELECT + WHERE
        sql_obj = conn.execute(sql, tup)
        res = self.sql_tuples_to_dict(sql_obj)
        return res

    def search_table(self, table_name, on_var, query):
        """Method to query an arbitrary table based.
        *** Do not let user define table_name or on_var as this would easily allow an injection attack!***

        Args:
            table_name string: name of table to query 
            on_var string: name of column to query one
            query string: value to match on on_var

        Returns:
            List[dict]: List where each entry is a dictionary Key: column name value: column value
        """
        conn = self.get_connection()
        SELECT = f"SELECT * FROM {table_name}"
        WHERE  = f" WHERE UPPER({on_var}) LIKE UPPER(%s);"
        query = f"%{query}%"
        tup = (query, )
        sql = SELECT + WHERE
        sql_obj = conn.execute(sql, tup)
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
        conn = self.get_connection()
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
    
    """ Method used for converting a column into being a reference column and populating it's reference table"""
    def create_reference_table(self, old_table_name, lookup_table_name, old_column, new_column_lookup_table, id_column):
        conn = self.get_connection()
        sql_unique = f"""SELECT distinct {old_column} FROM {old_table_name} WHERE {old_column} IS NOT NULL;"""
        res = conn.execute(sql_unique)
        vals = self.sql_obj_to_list(res)
        for val in vals:
            sql_popuale_new_table = f"""INSERT INTO {lookup_table_name} (`{new_column_lookup_table}`) VALUES ('{val}');"""
            conn.execute(sql_popuale_new_table)
        
        conn = self.get_connection()
        sql_get_mapping = f"""SELECT * FROM {lookup_table_name};"""
        result = conn.execute(sql_get_mapping)
        mapping = self.sql_tuples_to_dict(result)
        for element in mapping:
            id = element[id_column]
            name = element[new_column_lookup_table]
            sql = f"""UPDATE {old_table_name} SET {old_table_name}.{id_column} = {id} WHERE {old_table_name}.{old_column} = '{name}';"""
            conn.execute(sql)




    def create_public_table_runs(self, df_run, antibody_dict):
        tissue_slide_df = df_run.copy()
        tissue_cols = ["species", "tissue_type", "organ", "experimental_condition"]
        tissue_slide_df = tissue_slide_df[tissue_cols]
        sql = "SELECT MAX(tissue_id) FROM tissue_slides;"
        res = self.connection.execute(sql)
        tup = res.fetchone()
        max_id = tup[0]

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
        antibody_df = pd.DataFrame(antibody_dict.items(), columns=["epitope"])
        antibody_df["antibody_id"] = antibody_df.index
        return antibody_df


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
        conn = self.get_connection()
        sql = "DELETE FROM " + table_name + ";"
        conn.execute(sql)
        df.to_sql(table_name, self.engine, index=False, if_exists="append")

