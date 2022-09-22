import mysql.connector
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
        self.initialize()
        self.initEndpoints()
        self.path_db = Path(self.auth.app.config["DBPOPULATION_DIRECTORY"])
    def initialize(self):
        try:
            self.client = mysql.connector.connect(user=self.username, password=self.password,  host=self.host, port=self.port, database = self.db)
            self.cursor = self.client.cursor()
        except Exception as e:
            print(e)
            print("Unable to connect to DB.")

    def initEndpoints(self):

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

        @self.auth.app.route("/api/v1/run_db/repopulate_database", methods=["GET"])
        @self.auth.login_required
        def _populatedb():
            status_code = 200
            try:
                (df_content, df_content_mixed) = self.pull_table("Content")
                (df_results, df_results_mixed) = self.pull_table("Result")
                (df_experiment_run_step, experiment_run_step_mixed) = self.pull_table("ExperimentRunStep")

                df_tissue_meta = self.create_meta_table(df_content, df_content_mixed)
                df_bfx_results = self.create_bfx_table(df_content, df_results)
                df_flow_results = self.create_flow_table(df_content, df_results, df_experiment_run_step)

                self.write_df(df_tissue_meta, "dbit_metadata")
                self.write_df(df_bfx_results, "dbit_bfx_results")
                self.write_df(df_flow_results, "dbit_flow_results")
                # resp = Response("Success", status=status_code)
            except Exception as e:
                print(e)
                status_code = 500
                # resp = Response("Failure", status=status_code)
            return "done"

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

    def map_vals_dict(self, df, colname, dict):
        unique = pd.unique(df[colname])
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

    def create_meta_table(self, df_content, df_content_mixed):
        df_content = df_content.astype({"cntn_fk_status": str, "cntn_fk_contentType": str, "cntn_createdOn": int})
        ngs_cols = df_content
        #taking all ngs libraries that have runids and are sequenced
        ngs = ngs_cols[(ngs_cols.cntn_fk_status == '55') & (ngs_cols.cntn_cf_runId.notnull())& (ngs_cols.cntn_cf_runId != "None") & (ngs_cols.cntn_fk_contentType == '5')]
        slides = df_content[(df_content.cntn_fk_contentType == '42')]
        ngs_slide = pd.merge(left=ngs, right=slides, on="cntn_cf_runId", how="left", suffixes=("_NGS", ""))
        block_cols = df_content 
        tissue_block = block_cols[(block_cols.cntn_fk_contentType == '41')]

        ngs_slide["cntn_fk_originalContent"] = ngs_slide["cntn_fk_originalContent"].replace(["None"], -1)
        ngs_slide["cntn_fk_originalContent"] = pd.to_numeric(ngs_slide["cntn_fk_originalContent"])

        block_ngs = pd.merge(left=ngs_slide, right=tissue_block, left_on="cntn_fk_originalContent", right_on="pk", how = "left", suffixes=("", "_block"))

        cols = ["cntn_id_NGS", "cntn_cf_runId", "cntn_createdOn_NGS", "cntn_cf_fk_tissueType","cntn_cf_fk_organ", "cntn_cf_fk_species", "cntn_cf_experimentalCondition", "cntn_cf_sampleId",  "cntn_cf_disease", "cntn_cf_tissueSlideExperimentalCondition", "cntn_cf_source"]
        tissue = block_ngs[cols]
        species_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_species")
        organ_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_organ")
        tissueType_mapping = self.get_mapping_var_renaming_dict(df_content_mixed, "cntn_cf_fk_tissueType")
        tissue = self.map_vals_dict(tissue, "cntn_cf_fk_species", species_mapping)
        tissue = self.map_vals_dict(tissue, "cntn_cf_fk_organ", organ_mapping)
        tissue = self.map_vals_dict(tissue, "cntn_cf_fk_tissueType", tissueType_mapping)

        tissue = self.convert_dates(tissue, "cntn_createdOn_NGS")
        tissue["web_object_available"] = False

        web_objs = set()
        path = self.path_db.joinpath("ngids_with_webobjs.csv")
        with open(path, "r") as web_obj_csv:
            web_reader = csv.reader(web_obj_csv)
            inx = 0
            for row in web_reader:
                if inx > 0:
                    web_objs.add(row[0])
                inx += 1
        new_col = {
            "web_object_available": []
        }
        web_obs_vals = []
        for val in tissue["cntn_id_NGS"]:
            if val in web_objs:
                web_obs_vals.append(True)
                new_col["web_object_available"].append(True)
            else:
                web_obs_vals.append(False)
                new_col["web_object_available"].append(False)
        tissue["web_object_available"] = web_obs_vals
        # tissue.loc[tissue["cntn_id_NGS"] in web_objs, 'web_object_available'] = True
        return tissue
    
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

    def write_df(self, df, table_name):
        engine = db.create_engine("mysql+pymysql://root:atx!cloud!pw@api.atlasxomics.com:3306/dbit_data")
        sql = "DELETE FROM " + table_name + ";"
        engine.execute(sql)
        df.to_sql(table_name, engine, index=False, if_exists="append")

