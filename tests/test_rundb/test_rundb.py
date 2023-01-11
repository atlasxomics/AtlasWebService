import pytest
import unittest
from unittest.mock import patch
from src.rundb import MariaDB

def wipe_table(engine, table_name):
    sql = "DELETE FROM {}".format(table_name)
    conn = engine.connect()
    conn.execute(sql)

# def test_wipe_tables(setup_engine):
#     tables = ["job_table", "publications", "user_table", "groups_table", "tissue_slides", "results_studies", "results_metadata", "studies"]
#     for table in tables:
#         sql = "DELETE FROM {}".format(table)
#         conn = setup_engine.connect()
#         conn.execute(sql)
#         sql_select = "SELECT * FROM {}".format(table)
#         res = conn.execute(sql_select).fetchall()
#         assert res == []
        


def test_get_jobs_endpoint_single(run_db_api):
    sql, tup = run_db_api.generate_get_jobs_sql(run_id="DRunid")
    print(sql)
    sql_correct = f"SELECT * FROM {run_db_api.run_job_view} WHERE run_id = %s"
    print(sql_correct)
    assert sql.strip() == sql_correct.strip()
    assert tup == ("DRunid",)
    
    sql, tup = run_db_api.generate_get_jobs_sql(job_name="name1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE job_name = %s".strip()
    assert tup == ("name1",)
    
    sql, tup = run_db_api.generate_get_jobs_sql(username = "user1")
    assert sql.strip() == f"SELECT * FROM {run_db_api.run_job_view} WHERE username = %s".strip()
    assert tup == ("user1",)
    
    
def test_get_jobs_endpoint_multi(run_db_api):
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
    
    
     
    
    
    
