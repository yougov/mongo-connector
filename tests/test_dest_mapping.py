'''
Created on Jul 18, 2016

@author: ewxilli
'''
from tests import unittest
from mongo_connector.dest_mapping import DestMapping
from mongo_connector import errors

class TestDestMapping(unittest.TestCase):
    def setup_mapping(self, include, exclude, mapping):
        self.mapping = DestMapping(include, exclude, mapping)
        
    def test_default(self):
        # By default, all namespaces are kept without renaming
        self.setup_mapping([], [], {})
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "db1.col1")
        self.assertFalse(self.mapping.get_key("db1.col1"))
        self.assertListEqual(self.mapping.map_db("db1"), ["db1"])
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        
    def test_plain_in_no_ex_no_map(self):
        self.setup_mapping(["db1.col1", "db1.col2", "db1.col3"], [], {})
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.get("db1.col2", "db1.col2"), "db1.col2")
        self.assertEqual(self.mapping.get("db1.col3", "db1.col3"), "db1.col3")
        self.assertEqual(self.mapping.get_key("db1.col1"), "db1.col1")
        self.assertListEqual(self.mapping.map_db("db1"), ["db1"])
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db1.col4"))
        
    def test_wildcard_in_no_ex_no_map(self):
        self.setup_mapping(["db1.*"], [], {})
        self.assertFalse(self.mapping.get_key("db1.col1"))
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.get_key("db1.col1"),"db1.col1")
        self.assertListEqual(self.mapping.map_db("db1"), ["db1"])
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db2.col4"))
        
    def test_yes_in_yes_ex_no_map(self):
        self.setup_mapping(["db1.*"], ["db1.col4"], {})
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db1.col4"))
        
    def test_init_mapping(self):
        mapping = {
          "db1_*.col1": "newdb1.newcol1"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)           
            
        mapping = {
          "db1.*": "newdb*_*.newcol"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)
        
        mapping = {
          "db1.col1": "newdb.newcol",
          "db2.col1": "newdb.newcol"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)
        
        mapping = {
          "db1.*": "newdb1_*.newcol",
          "db2.a": "newdb2_a.b"
        }
        self.setup_mapping([], [], mapping)
        self.assertDictEqual(self.mapping.plain, {"db2.a":"newdb2_a.b"})
        self.assertDictEqual(self.mapping.plain_db, {"db2":["newdb2_a"]})
        self.assertDictEqual(self.mapping.reverse_plain, {"newdb2_a.b":"db2.a"})
        self.assertDictEqual(self.mapping.wildcard, {"db1.*":"newdb1_*.newcol"})
        
    def test_map(self):
        include = ["db1.*"]
        exclude = ["db1.col4"]
        mapping = {
          "db1.*": "newdb1_*.colx"
        }
        self.setup_mapping(include, exclude, mapping)
        self.assertDictEqual(self.mapping.plain, {})
        self.assertDictEqual(self.mapping.wildcard, {"db1.*": "newdb1_*.colx"})
        
        # when we get a matched map, plain should contain that one afterwards
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "newdb1_col1.colx")
        self.assertDictEqual(self.mapping.plain, {"db1.col1": "newdb1_col1.colx"})
        
        # when we get a matched map, plain should contain that one afterwards
        self.assertEqual(self.mapping.get("db1.col2", "db1.col2"), "newdb1_col2.colx")
        self.assertDictEqual(self.mapping.plain, {"db1.col1": "newdb1_col1.colx", "db1.col2": "newdb1_col2.colx"})
        
        self.assertEqual(self.mapping.get_key("newdb1_col1.colx"), "db1.col1")
        self.assertSetEqual(set(self.mapping.map_db("db1")),set(["newdb1_col1", "newdb1_col2"]))
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "newdb1_col1.colx")
        
        

if __name__ == '__main__':
    unittest.main()
    