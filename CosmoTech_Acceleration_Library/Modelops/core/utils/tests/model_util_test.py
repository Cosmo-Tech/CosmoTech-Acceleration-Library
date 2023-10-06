# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import unittest

from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil


class TestModelUtil(unittest.TestCase):
    # Global variables
    simple_parameters = {
        "id": "Twin1",
        "brand": "Ford",
        "electric": False,
        "year": 1964,
        "dict_param": {
            "property1": "toto",
            "property2": "tata",
        },
        "with_quotes": "'9999'",
        "with_dbl_quotes": '"1234"',
        "colors": ["red", "white", "blue"]
    }

    relationship_simple_parameters = {
        "src": "Node1",
        "dest": "Node2",
        "brand": "Ford",
        "electric": False,
        "year": 1964,
        "dict_param": {
            "property1": "toto",
            "property2": "tata",
        },
        "with_quotes": "'12345'",
        "colors": ["red", "white", "blue"]
    }

    dict_with_simple_json_string = {
        "src": "Node1",
        "dest": "Node2",
        "brand": "Ford",
        "electric": False,
        "year": 1964,
        "dict_param": "{\"property1\": \"toto\", \"property2\": \"tata\"}",
        "with_quotes": "'12345'",
        "colors": ["red", "white", "blue"]
    }

    expected_simple_parameters = '{id : "Twin1", ' \
                                 'brand : "Ford", ' \
                                 'electric : False, ' \
                                 'year : 1964, ' \
                                 'dict_param : {property1:\"toto\",property2:\"tata\"}, ' \
                                 'with_quotes : "\'9999\'", ' \
                                 'with_dbl_quotes : "\\"1234\\"", ' \
                                 'colors : ["red","white","blue"]}'

    expected_relationship_simple_parameters = '{src : "Node1", ' \
                                              'dest : "Node2", ' \
                                              'brand : "Ford", ' \
                                              'electric : False, ' \
                                              'year : 1964, ' \
                                              'dict_param : {property1:\"toto\",property2:\"tata\"}, ' \
                                              'with_quotes : "\'12345\'", ' \
                                              'colors : ["red","white","blue"]}'

    def setUp(self):
        self.model_util = ModelUtil()

    def test_dict_to_cypher_parameters_with_simple_parameters(self):
        self.assertEqual(self.expected_simple_parameters,
                         self.model_util.dict_to_cypher_parameters(self.simple_parameters))

    def test_create_index_query(self):
        expected_result = "CREATE INDEX ON :Entity_Test(property_name_test)"
        self.assertEqual(expected_result, self.model_util.create_index_query("Entity_Test", "property_name_test"))

    def test_create_twin_query(self):
        expected_result = f"CREATE (:Entity_Test {self.expected_simple_parameters})"
        self.assertEqual(expected_result, self.model_util.create_twin_query("Entity_Test", self.simple_parameters))

    def test_create_twin_query_Exception(self):
        twin_name = 'Twin_name'
        self.assertRaises(Exception, self.model_util.create_twin_query, twin_name, self.expected_simple_parameters)

    def test_create_relationship_query(self):
        source_id = 'Node1'
        destination_id = 'Node2'
        relation_name = 'Relation_Name'
        expected_result = f"MATCH (n), (m) WHERE n.{ModelUtil.dt_id_key} = '{source_id}' AND m.{ModelUtil.dt_id_key} = '{destination_id}' CREATE (n)-[r:{relation_name} {self.expected_relationship_simple_parameters}]->(m) RETURN r"
        self.assertEqual(expected_result,
                         self.model_util.create_relationship_query(relation_name, self.relationship_simple_parameters))

    def test_create_relationship_query_Exception(self):
        relation_name = 'Relation_Name'
        self.assertRaises(Exception, self.model_util.create_relationship_query, relation_name,
                          self.expected_simple_parameters)


if __name__ == '__main__':
    unittest.main()
