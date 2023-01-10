import json


def test_s3_client(testing_app):
    gene = testing_app.config["SUBMODULES"]["GeneAPI"]
    assert gene is not None
