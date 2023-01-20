import pytest
import unittest
from unittest.mock import patch
from src.rundb import MariaDB
import sqlalchemy as db
import pymysql




def wipe_table(engine, table_name):
    sql = "DELETE FROM {}".format(table_name)
    conn = engine.connect()
    conn.execute(sql)

def add_run_id(engine, run_id):
    sql = "INSERT INTO tissue_slides (run_id) VALUES (%s)"
    conn = engine.connect()
    conn.execute(sql, (run_id,))
    
def get_tissue_id(engine, run_id):
    sql = "SELECT tissue_id FROM tissue_slides WHERE run_id = %s"
    conn = engine.connect()
    res = conn.execute(sql, (run_id,)).fetchone()
    print(res)
    if res:
        return res[0]
    return None
# def wipe_db(engine):
#     table_list = [ "groups_table", "study_tissue_table", "tissue_source_table", "tissue_type_table", "tissue_slides" ,
#                   "user_group_table", "user_table", "job_tissue_id_table" ,"job_table",
#                   "results_metadata", "user_group_table","study_tissue_table" ,"study_table" ]
                  
#     for table in table_list:
#         wipe_table(engine, table)

def test_get_jobs_endpoint_single(run_db_api):
    sql, tup = run_db_api.generate_get_jobs_sql(run_id="DRunid")
    sql_correct = f"SELECT * FROM {run_db_api.run_job_view} WHERE run_id = %s"
    assert sql.strip() == sql_correct.strip()
    assert tup == ("DRunid",)
    
    sql, tup = run_db_api.generate_get_jobs_sql(job_name="name1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE job_name = %s".strip()
    assert tup == ("name1",)
    
    sql, tup = run_db_api.generate_get_jobs_sql(username = "user1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE username = %s".strip()
    assert tup == ("user1",)
    
    
def test_get_jobs_endpoint_multi(run_db_api):
    print(run_db_api)
    sql, tup = run_db_api.generate_get_jobs_sql(run_id="id1", job_name="job1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE job_name = %s AND run_id = %s".strip()
    assert tup == ("job1", "id1")
    
    sql, tup = run_db_api.generate_get_jobs_sql(run_id="id1", username="user1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE username = %s AND run_id = %s".strip()
    assert tup == ("user1", "id1")
    
    sql, tup = run_db_api.generate_get_jobs_sql(job_name="job1", username="user1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE username = %s AND job_name = %s".strip()
    assert tup == ("user1", "job1")
    
    sql, tup = run_db_api.generate_get_jobs_sql(run_id="id1", job_name="job1", username="user1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE username = %s AND job_name = %s AND run_id = %s".strip()
    assert tup == ("user1", "job1","id1" )
    

def test_grab_runs_homepage_groups_sql(run_db_api):
    groups = ["group1", "group2"]
    tup, res = run_db_api.grab_runs_homepage_groups_sql(groups)
    assert tup == ("group1", "group2")
    assert res == f"SELECT * FROM {run_db_api.homepage_population_name} WHERE `group` IN (%s, %s) OR public = 1;"
    groups = ["group1"]
    tup, res = run_db_api.grab_runs_homepage_groups_sql(groups)
    assert tup == ("group1",)
    assert res == f"SELECT * FROM {run_db_api.homepage_population_name} WHERE `group` IN (%s) OR public = 1;"
    
    groups = []
    tup, res = run_db_api.grab_runs_homepage_groups_sql(groups)
    assert tup == ()
    assert res == f"SELECT * FROM {run_db_api.homepage_population_name} WHERE public = 1;"
   
@patch("src.rundb.MariaDB.get_connection") 
def test_create_study(get_connection_mock, run_db_api, mock_engine):
    conn = mock_engine.connect()
    get_connection_mock.return_value = conn
    
    # wipe_db(mock_engine)
    wipe_table(mock_engine, "study_tissue_table")
    wipe_table(mock_engine, "tissue_slides")
    wipe_table(mock_engine, "study_table")
    
    study_name = "test_study1"
    study_description = "test_description1"
    
    id = run_db_api.create_study(study_name, study_description)
    assert id != None
    assert type(id) == int
    
@patch("src.rundb.MariaDB.get_connection")
def test_add_study_description(get_connection_mock, run_db_api, mock_engine):
    conn = mock_engine.connect()
    get_connection_mock.return_value = conn
    
    study_description = "test_description2"
    study_name = "test_study1"
    sql = "SELECT study_description FROM study_table WHERE study_id = %s"
    id = run_db_api.get_study_id_from_name(study_name)
    
    res1 = conn.execute(sql, (id, )).fetchone()
    assert res1[0] != study_description
    
    run_db_api.add_study_description(id, study_description)
    res = conn.execute(sql, (id, )).fetchone()
    
    assert res[0] == study_description
    
@patch("src.rundb.MariaDB.get_connection")
def test_add_study_run(mock_connection, run_db_api, mock_engine):
    conn = mock_engine.connect()
    mock_connection.return_value = conn
    
    study_name = "test_study1"
    run_id = "test_run1"

    study_id = run_db_api.get_study_id_from_name(study_name)
    
    add_run_id(mock_engine, run_id)
    tissue_id = get_tissue_id(mock_engine, run_id)
    print(tissue_id)
    current_ids = run_db_api.get_study_runs(study_id)
    assert current_ids == []
    
    run_db_api.add_study_run(study_id, tissue_id)
    current_ids = run_db_api.get_study_runs(study_id)
    assert current_ids == [{ 'run_id': run_id, 'tissue_id': tissue_id }]
    
@patch("src.rundb.MariaDB.get_connection")
def test_remove_study_run(mock_connection, run_db_api, mock_engine):
    conn = mock_engine.connect()
    mock_connection.return_value = conn
    
    study_name = "test_study1"
    run_id = "test_run1"

    study_id = run_db_api.get_study_id_from_name(study_name)
    tissue_id = run_db_api.get_tissue_id_from_run_id(run_id)
    
    run_db_api.remove_study_run(study_id, tissue_id)
    current_ids = run_db_api.get_study_runs(study_id)
    assert current_ids == []

@patch("src.rundb.MariaDB.get_connection")
def test_update_study_table(mock_connection, run_db_api, mock_engine):
    conn = mock_engine.connect()
    mock_connection.return_value = conn
    
    add_run_id(mock_engine, "test_run2")
    add_run_id(mock_engine, "test_run3")
    add_run_id(mock_engine, "test_run4")
    add_run_id(mock_engine, "test_run5")
    add_run_id(mock_engine, "test_run6")
    add_run_id(mock_engine, "test_run7")
    
    tissue_id1 = run_db_api.get_tissue_id_from_run_id("test_run1")
    tissue_id2 = run_db_api.get_tissue_id_from_run_id("test_run2")
    tissue_id3 = run_db_api.get_tissue_id_from_run_id("test_run3")
    tissue_id4 = run_db_api.get_tissue_id_from_run_id("test_run4")
    tissue_id5 = run_db_api.get_tissue_id_from_run_id("test_run5")
    tissue_id6 = run_db_api.get_tissue_id_from_run_id("test_run6")
    tissue_id7 = run_db_api.get_tissue_id_from_run_id("test_run7")
    
    p1 = { 'run_id': "test_run1", 'tissue_id': tissue_id1 }
    p2 = { 'run_id': "test_run2", 'tissue_id': tissue_id2 }
    p3 = { 'run_id': "test_run3", 'tissue_id': tissue_id3 }
    p4 = { 'run_id': "test_run4", 'tissue_id': tissue_id4 }
    p5 = { 'run_id': "test_run5", 'tissue_id': tissue_id5 }
    p6 = { 'run_id': "test_run6", 'tissue_id': tissue_id6 }
    p7 = { 'run_id': "test_run7", 'tissue_id': tissue_id7 }
    
    #modifying existing study
    study_name = "test_study1"
    study_description = "test_description3"
    
    study_id = run_db_api.get_study_id_from_name(study_name)
    adding_list = [p1, p2, p3, p4]
    run_db_api.update_study_table(study_id, study_name, study_description, adding_list, [])
    
    runs = run_db_api.get_study_runs(study_id)
    
    assert len(runs) == 4
    assert runs == [p1, p2, p3, p4]
    
    adding_list = [p5]
    removing_list = [p1, p2]
    run_db_api.update_study_table(study_id, study_name, study_description, adding_list, removing_list)
    
    runs = run_db_api.get_study_runs(study_id)
    assert len(runs) == 3
    assert runs == [p3, p4, p5]
    
    #new study
    adding_list = [p5, p6, p7]
    removing_list = []
    study_name = "test_study2"
    study_id = None
    study_description = "test_description4"
    run_db_api.update_study_table(study_id, study_name, study_description, adding_list, removing_list)
    
    study_id = run_db_api.get_study_id_from_name(study_name)
    
    runs = run_db_api.get_study_runs(study_id)
    assert len(runs) == 3
    
    assert runs == [p5, p6, p7]
    
    removing_list = [p5, p6, p7]
    adding_list = []
    run_db_api.update_study_table(study_id, study_name, study_description, adding_list, removing_list)
    
    runs = run_db_api.get_study_runs(study_id)
    assert len(runs) == 0
    assert runs == []
    
    
    
