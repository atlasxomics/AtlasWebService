import json
import unittest
from unittest.mock import patch
from src.genes import GeneAPI
import pytest

@patch('src.genes.GeneAPI.getFileObject')
@pytest.mark.parametrize('file_path', ['tests/test_genes/test_genes_files/test_data.csv.gz', 'tests/test_genes/test_genes_files/test_data.csv'])
def test_get_spatialData_gz(mock_getFileObject, file_path,testing_gene_api):
    mock_getFileObject.return_value = file_path
    res = testing_gene_api.get_SpatialData({'filename': file_path})
    assert res == [ ['C1', '10', '10', '20', '20', '30', '30'],
    ['C2', '11', '14', '20', '25', '31', '30'],
    ['C1', '12', '15', '20', '25', '32', '34'],
    ['C3', '13', '15', '20', '26', '33', '30'],]