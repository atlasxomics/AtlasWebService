import os
import sys
import json
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