import os
import sys
import json
import sqlalchemy as db

topdir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(topdir)

from src import app
import pytest

@pytest.fixture()
def client_testing():
    app.config['TESTING'] = True
    client = app.test_client()
    yield client

@pytest.fixture()
def testing_app():
    app.config['TESTING'] = True
    yield app

@pytest.fixture()
def testing_admin_header(client_testing):
    res = client_testing.post('/api/v1/auth/login', data = json.dumps({'username': 'admin', 'password':'Hello123!'}), headers = {'Content-Type': 'application/json'})
    result = json.loads(res.data)
    token = "JWT {}".format(result['access_token'])
    header = { 'Authorization': token , 'Content-Type': 'application/json' }
    return header

@pytest.fixture()
def testing_gene_api(testing_app):
    gene = testing_app.config["SUBMODULES"]["GeneAPI"]
    return gene

@pytest.fixture()
def testing_storage_api(testing_app):
    storage = testing_app.config["SUBMODULES"]["StorageAPI"]
    return storage

@pytest.fixture()
def run_db_api(testing_app):
    rundb = testing_app.config["SUBMODULES"]["RelationalDatabaseAPI"]
    return rundb

@pytest.fixture()
def setup_engine(testing_app):
    auth = testing_app.config["SUBMODULES"]["Auth"]
    connection_string = """mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}""".format(
        username=testing_app.config["MYSQL_USERNAME"], password=testing_app.config["MYSQL_PASSWORD"],
        host=testing_app.config["MYSQL_HOST"], port=str(testing_app.config["MYSQL_PORT"]), dbname="mock_db")
    engine = db.create_engine(connection_string)
    return engine
    