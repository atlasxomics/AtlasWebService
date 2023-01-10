import json
import unittest
from unittest.mock import patch
from src.storage import StorageAPI
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
    with pytest.raises(Exception):
        res = testing_storage_api.generatePresignedUrls( {"item1": { "bucket": "bucket1"}})
    
    with pytest.raises(Exception):
        res = testing_storage_api.generatePresignedUrls( {"item1": { "path": "path1"}})
    
    assert mock_generatePresignedURL.call_count == 3