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
            connection_string = "mysql+pymysql://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.db
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

        @self.auth.app.route("/api/v1/run_db/repopulate_database", methods=["GET"])
        @self.auth.login_required
        def _populatedb():
            status_code = 200
            try:
                #uncomment for local testing
                (df_results, df_results_mixed) = self.pull_table("Result")
                (df_experiment_run_step, experiment_run_step_mixed) = self.pull_table("ExperimentRunStep")
                (df_content, df_content_mixed) = self.pull_table("Content")
                # df_content.to_csv("content.csv")
                # df_content_mixed.to_csv("content_mixed.csv")
                # df_results.to_csv("Result.csv")
                # df_results_mixed.to_csv("Results_mixed.csv")
                # df_experiment_run_step.to_csv("ExperimentRunStep.csv")
                # experiment_run_step_mixed.to_csv("ExperimentRunStepMixed.csv")
                # print("tables pulled")
                # df_content = pd.read_csv("Content.csv")
                # df_content_mixed = pd.read_csv("content_mixed.csv")
                # df_results = pd.read_csv("Result.csv")
                # df_experiment_run_step = pd.read_csv("ExperimentRunStep.csv")

                df_tissue_meta = self.create_meta_table(df_content, df_content_mixed)
                df_bfx_results = self.create_bfx_table(df_content, df_results)
                df_flow_results = self.create_flow_table(df_content, df_results, df_experiment_run_step)

                self.write_df(df_tissue_meta, "dbit_metadata")
                self.write_df(df_bfx_results, "dbit_bfx_results")
                self.write_df(df_flow_results, "dbit_flow_results")
                status = "Success"
            except Exception as e:
                print(e)
                status_code = 200
                status = "Failure"
            finally:
                self.write_update(status)
                resp = Response(status, status=status_code)
                return resp

        @self.auth.app.route("/api/v1/run_db/get_last_update", methods=["GET"])
        @self.auth.admin_required
        def _get_repopulation_date():
            print('here')
            sc = 200
            try:
                row = self.get_latest_date()
                dic = {
                    'date': row[1],
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
        print(row)
        return row

    def get_ngs_ids(self):
        sql = '''SELECT ngs_id FROM dbit_metadata WHERE web_object_available = 1;
        '''
        res = self.connection.execute(sql)
        ids = res.fetchall()
        ids_final = []
        for i in range(len(ids)):
            ng_id = ids[i][0]
            ids_final.append({'id': ng_id})
        print(ids_final)
        return ids_final

    def write_update(self ,status):
        sql1 =  "SELECT MAX(inx) FROM dbit_data_repopulations;"
        res = self.connection.execute(sql1)
        prev_inx = res.fetchone()[0]
        new_inx = prev_inx + 1
        current_date = str(datetime.datetime.now())
        period_inx = current_date.find('.')
        current_date = current_date[:period_inx]
        current_date = current_date.replace(' ', '-')
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
        df_content = df_content.astype({"cntn_fk_status": str, "cntn_fk_contentType": str, "cntn_createdOn": int, "cntn_fk_status": str, "cntn_cf_fk_workflow": str})
        ngs_cols = df_content
        #taking all ngs libraries that have runids and are sequenced
        ngs = ngs_cols[(ngs_cols.cntn_fk_status == '55') & (ngs_cols.cntn_cf_runId.notnull())& (ngs_cols.cntn_cf_runId != "None") & (ngs_cols.cntn_fk_contentType == '5')]
        slides = df_content[(df_content.cntn_fk_contentType == '42') & (df_content.cntn_fk_status != '54')]
        ngs_slide = pd.merge(left=ngs, right=slides, on="cntn_cf_runId", how="left", suffixes=("_NGS", ""))
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

        # print(tissue.cntn_cf_fk_workflow)
        block_ngs = self.convert_dates(block_ngs, "cntn_createdOn_NGS")
        # block_ngs["web_object_available"] = False
        web_objs = set()
        path = self.path_db.joinpath("ngids_with_webobjs.csv")
        with open(path, "r") as web_obj_csv:
            web_reader = csv.reader(web_obj_csv)
            inx = 0
            for row in web_reader:
                if inx > 0:
                    web_objs.add(row[0])
                inx += 1
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
        sql = "DELETE FROM " + table_name + ";"
        self.connection.execute(sql)
        df.to_sql(table_name, self.engine, index=False, if_exists="append")


