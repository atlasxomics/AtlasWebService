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
    with pytest.raises(Exception):
        res = testing_storage_api.generatePresignedUrls( {"item1": { "bucket": "bucket1"}})
    
    with pytest.raises(Exception):
        res = testing_storage_api.generatePresignedUrls( {"item1": { "path": "path1"}})
    
    assert mock_generatePresignedURL.call_count == 3

def test_getFileList(testing_app):
    auth = Auth(testing_app)
    mong = MongoDB(testing_app)
    obj = StorageAPI(auth, mong)
    patch.object(obj, 'aws_s3', return_value=MagicMock())
    obj.aws_s3.return_value.get_paginator.return_value = MagicMock()
    obj.aws_s3.return_value.get_paginator.return_value.paginate.return_value = "foo"
    # mock_client.return_value.get_paginator.return_value.paginate.return_value = [
    #     {"Contents": [
    #         { "Key": "root/abba"},
    #         { "Key": "abba2"},
    #         { "Key": "root/path/to/yard" },
    #         { "Key": "root/folder/"},
    #         { "Key": "root/folder/abba"},
    #                   ] },
    # ]
    res1 = obj.getFileList(bucket_name="bucket", root_path = "root", fltr = ["abba"],delimiter=None, only_files=False)
    assert res1 == ["root/abba", "root/folder/abba"]
    



# @patch('src.storage.StorageAPI.aws_s3')
# def test_client_patch(mock_client, testing_storage_api):
#     mock_client.generate_presigned_url.return_value = "http:fake_link"
#     res = testing_storage_api.generatePresignedUrl("bucket", "key")
#     print(res)
#     assert 1 == 2
    