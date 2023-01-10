import json


def test_s3_client(testing_app):
    gene = testing_app.config["SUBMODULES"]["GeneAPI"]
    print(gene)
    assert 1 == 2
