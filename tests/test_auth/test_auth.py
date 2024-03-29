import json
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock
from src.auth import Auth
import pytest

def wipe_table(engine, table_name):
    sql = "DELETE FROM {}".format(table_name)
    conn = engine.connect()
    conn.execute(sql)
    


@patch('src.auth.Auth.get_connection')
def test_get_user_id(mock_get_connection, testing_auth_api, mock_engine):
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    wipe_table(mock_engine, "user_logins")
    wipe_table(mock_engine, "user_table")
    
    user_id1 = "test_user1"
    sql = "SELECT * FROM user_table WHERE username = %s" 
    res = conn.execute(sql, (user_id1,)).fetchone()
    assert res is None
    
    placed_id = testing_auth_api.get_user_id(user_id1)
    res = conn.execute(sql, (user_id1,)).fetchone()
    found_id = res[0]
    
    assert found_id == placed_id
    
    found_id2 = testing_auth_api.get_user_id(user_id1)
    assert found_id2 == found_id

@patch('src.auth.Auth.get_connection')
def test_get_group_id(mock_get_connection, testing_auth_api, mock_engine):
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    
    wipe_table(mock_engine, "groups_table")
    group_name1 = "test_group1"
    sql = "SELECT * FROM groups_table WHERE group_name = %s" 
    res = conn.execute(sql, (group_name1,)).fetchone()
    assert res is None
    
    insert_id = testing_auth_api.get_group_id(group_name1)
    res = conn.execute(sql, (group_name1,)).fetchone()
    found_id = res[0]
    assert found_id == insert_id
    
    func_id = testing_auth_api.get_group_id(group_name1)
    assert func_id == found_id
    
@patch('src.auth.Auth.get_connection')
def test_assign_group_sql(mock_get_connection, testing_auth_api, mock_engine):
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    
    wipe_table(mock_engine, "user_group_table")
    username = "test_user1"
    group_name = "test_group1"
    
    user_id = testing_auth_api.get_user_id(username)
    group_id = testing_auth_api.get_group_id(group_name)
    sql = "SELECT * FROM user_group_table WHERE user_id = %s AND group_id = %s" 
    res = conn.execute(sql, (user_id, group_id)).fetchone()
    assert res is None
    
    testing_auth_api.assign_group_sql(username, group_name)
    res = conn.execute(sql, (user_id, group_id)).fetchone()
    assert res is not None
    
    username2 = "username2"
    group2 = "group2"
    testing_auth_api.assign_group_sql(username2, group2)
    sql_check_username_added = "SELECT * FROM user_table WHERE username = %s"
    sql_check_group_added = "SELECT * FROM groups_table WHERE group_name = %s"
    res_group = conn.execute(sql_check_group_added, (group2,)).fetchone()
    res_username = conn.execute(sql_check_username_added, (username2,)).fetchone()
    assert res_group is not None
    assert res_username is not None
    
    found_username_id = res_username[0]
    found_group_id = res_group[0]
    
    username2_id = testing_auth_api.get_user_id(username2)
    group2_id = testing_auth_api.get_group_id(group2)
    
    assert found_username_id == username2_id
    assert found_group_id == group2_id

@patch('src.auth.Auth.get_connection')
def test_remove_user_group_sql(mock_get_connection, testing_auth_api, mock_engine):
    
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    
    sql_check_user_group_table = "SELECT * FROM user_group_table"
    
    username1 = "test_user1"
    username2 = "username2"
    group1 = "test_group1"
    group2 = "group2"
    
    user1_id = testing_auth_api.get_user_id(username1)
    group1_id = testing_auth_api.get_group_id(group1)
    user2_id = testing_auth_api.get_user_id(username2)
    group2_id = testing_auth_api.get_group_id(group2)
    
    res = conn.execute(sql_check_user_group_table).fetchall()
    
    assert res == [(group1_id, user1_id), (group2_id, user2_id)]
    
    testing_auth_api.remove_user_group_sql(username1, group1)
    
    res = conn.execute(sql_check_user_group_table).fetchall()
    assert res == [(group2_id, user2_id)]
    
    testing_auth_api.remove_user_group_sql(username2, group2)
    
    res = conn.execute(sql_check_user_group_table).fetchall()
    assert res == []

@patch('src.auth.Auth.get_connection')
def test_user_logged(mock_get_connection,testing_auth_api, mock_engine):
    wipe_table(mock_engine, "user_logins")
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    username = "test_user1"
    testing_auth_api.document_login(username)
    user_id = testing_auth_api.get_user_id(username)
    sql_test = "SELECT * FROM user_logins WHERE user_id = %s"
    res = conn.execute(sql_test, (user_id,)).fetchall()
    assert res is not None

@patch('src.auth.Auth.get_connection')
@patch('src.auth.Auth.authenticate')
def test_user_login_functional(mock_authenticate, mock_get_connection, mock_engine, client_testing, testing_admin_header, testing_auth_api):
    conn = mock_engine.connect()
    mock_get_connection.return_value = conn
    mock_authenticate.return_value = ('username2', 'token')
    sql = "SELECT * FROM user_logins"
    res = conn.execute(sql).fetchall()
    client_testing.post('/api/v1/auth/login',data = json.dumps({'username': 'username2', 'password': 'foobar'}) ,headers=testing_admin_header)
    res2 = conn.execute(sql).fetchall()
    assert len(res2) == len(res) + 1
    
    user_id = testing_auth_api.get_user_id('username2')
    assert res2[-1][1] == user_id
    
    assert res[0][2] < res2[-1][2]
    
    
    