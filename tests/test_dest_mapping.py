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

    def test_only_include(self):
        # Test that only provides include namespaces

        # Test plain case
        self.setup_mapping(["db1.col1", "db1.col2", "db1.col3"], [], {})
        self.assertEqual(self.mapping.get("db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.get("db1.col2"), "db1.col2")
        self.assertEqual(self.mapping.get("db1.col3"), "db1.col3")
        self.assertEqual(self.mapping.get_key("db1.col1"), "db1.col1")
        self.assertListEqual(self.mapping.map_db("db1"), ["db1"])
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db1.col4"))

        # Test wildcard case
        self.setup_mapping(["db1.*"], [], {})
        self.assertFalse(self.mapping.get_key("db1.col1"))
        self.assertEqual(self.mapping.get("db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.get_key("db1.col1"), "db1.col1")
        self.assertListEqual(self.mapping.map_db("db1"), ["db1"])
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db2.col4"))

    def test_only_exclude(self):
        # Test that only provides exclude namespaces

        # Test plain case
        self.setup_mapping([], ["db1.col4"], {})
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db1.col4"))

        # Test wildcard case
        self.setup_mapping([], ["db2.*"], {})
        self.assertEqual(self.mapping.get("db1.col1", "db1.col1"), "db1.col1")
        self.assertEqual(self.mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertIsNone(self.mapping.map_namespace("db2.col"))

    def test_mapping(self):
        # mulitple dbs cannot be mapped to the same db
        mapping = {
          "db1.col1": "newdb.newcol",
          "db2.col1": "newdb.newcol"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping,
                          ["db1.col1", "db2.col1"], [], mapping)

        # Test mapping
        mapping = {
          "db1.*": "newdb1_*.newcol",
          "db2.a": "newdb2_a.x",
          "db2.b": "b_newdb2.x"
        }
        self.setup_mapping(["db1.*", "db2.a", "db2.b"], [], mapping)
        self.assertDictEqual(self.mapping.plain,
                             {"db2.a": "newdb2_a.x", "db2.b": "b_newdb2.x"})
        self.assertDictEqual(self.mapping.plain_db,
                             {"db2": set(["newdb2_a", "b_newdb2"])})
        self.assertDictEqual(self.mapping.reverse_plain,
                             {"newdb2_a.x": "db2.a", "b_newdb2.x": "db2.b"})
        self.assertDictEqual(self.mapping.wildcard,
                             {"db1.*": "newdb1_*.newcol"})
        self.assertEqual(self.mapping.get_key("newdb2_a.x"), "db2.a")
        self.assertSetEqual(set(self.mapping.map_db("db2")),
                            set(["newdb2_a", "b_newdb2"]))
        # when we get matched maps, plain should contain those ones afterwards
        self.assertEqual(self.mapping.get("db1.col1"), "newdb1_col1.newcol")
        self.assertEqual(self.mapping.map_namespace("db1.col2"),
                         "newdb1_col2.newcol")
        self.assertDictEqual(self.mapping.plain,
                             {"db2.a": "newdb2_a.x",
                              "db2.b": "b_newdb2.x",
                              "db1.col1": "newdb1_col1.newcol",
                              "db1.col2": "newdb1_col2.newcol"})
        self.assertDictEqual(self.mapping.plain_db,
                             {"db2": set(["newdb2_a", "b_newdb2"]),
                              "db1": set(["newdb1_col1", "newdb1_col2"])})
        self.assertDictEqual(self.mapping.reverse_plain,
                             {"newdb2_a.x": "db2.a",
                              "b_newdb2.x": "db2.b",
                              "newdb1_col1.newcol": "db1.col1",
                              "newdb1_col2.newcol": "db1.col2"})

if __name__ == '__main__':
    unittest.main()
