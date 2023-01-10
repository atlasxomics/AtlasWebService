import json

# from main import run_app
def test_populate_homepage(client_testing, testing_admin_header):
    """Test function."""
    res = client_testing.get('/api/v1/run_db/populate_homepage', headers=testing_admin_header)
    assert res.status_code == 200

def test_get_info_from_results_id(client_testing, testing_admin_header):
    data = { "results_id": 11048 }
    res = client_testing.post('/api/v1/run_db/get_info_from_results_id',data = json.dumps(data) ,headers=testing_admin_header)
    assert res.status_code == 200

    data = json.loads(res.data)
    assert data['results_id'] == 11048


