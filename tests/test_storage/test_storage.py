import json
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock
from src.storage import StorageAPI
from src.auth import Auth
from src.database import MongoDB
import pytest

def test_init_storage(testing_storage_api):
    assert testing_storage_api is not None
    assert testing_storage_api.bucket_name is not None

@patch('src.storage.StorageAPI.generatePresignedUrl')
def test_presigned_url_generation(mock_generatePresignedURL, testing_storage_api):
    mock_generatePresignedURL.return_value = "http:fake_link"
    paths = {
        "item1": { "bucket": "bucket1", "path": "key1"},
        "item2": { "bucket": "bucket2", "path": "key2"},
        "item3": { "bucket": "bucket3", "path": "key3"}
    }
    res = testing_storage_api.generatePresignedUrls(paths)
    assert res == {
        "item1": "http:fake_link",
        "item2": "http:fake_link",
        "item3": "http:fake_link"
    }
    
def testing_presigned_url_errors(testing_storage_api):
    path1 = "/path/to/file.txt"
    bucket = None
    
    with pytest.raises(Exception):
        testing_storage_api.generatePresignedUrl(path1, bucket)
    
    bucket = "bucket7"
    path2 = None
    
    with pytest.raises(Exception):
        testing_storage_api.generatePresignedUrl(path2, bucket)