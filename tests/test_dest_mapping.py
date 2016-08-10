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
        self.assertEqual(self.mapping.get("a.b", "a.b"), "a.b")
        self.assertFalse(self.mapping.get_key("a.b"))
        self.assertListEqual(self.mapping.map_db("a"), ["a"])
        self.assertEqual(self.mapping.map_namespace("a.b"), "a.b")        
        
    def test_plain_in_no_ex_no_map(self):
        self.setup_mapping(["eiffelevents.allevents", "eiffelevents.artifacts", "eiffelevents.baselines"], [], {})
        self.assertEqual(self.mapping.get("eiffelevents.allevents", "eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertEqual(self.mapping.get("eiffelevents.artifacts", "eiffelevents.artifacts"), "eiffelevents.artifacts")
        self.assertEqual(self.mapping.get("eiffelevents.baselines", "eiffelevents.baselines"), "eiffelevents.baselines")
        self.assertEqual(self.mapping.get_key("eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertListEqual(self.mapping.map_db("eiffelevents"), ["eiffelevents"])
        self.assertEqual(self.mapping.map_namespace("eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertIsNone(self.mapping.map_namespace("eiffelevents.jobs"))
        
    def test_wildcard_in_no_ex_no_map(self):
        self.setup_mapping(["eiffelevents.*"], [], {})
        self.assertFalse(self.mapping.get_key("eiffelevents.allevents"))
        self.assertEqual(self.mapping.get("eiffelevents.allevents", "eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertEqual(self.mapping.get_key("eiffelevents.allevents"),"eiffelevents.allevents")
        self.assertListEqual(self.mapping.map_db("eiffelevents"), ["eiffelevents"])
        self.assertEqual(self.mapping.map_namespace("eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertIsNone(self.mapping.map_namespace("testevents.jobs"))
        
    def test_yes_in_yes_ex_no_map(self):
        self.setup_mapping(["eiffelevents.*"], ["eiffelevents.jobs"], {})
        self.assertEqual(self.mapping.map_namespace("eiffelevents.allevents"), "eiffelevents.allevents")
        self.assertIsNone(self.mapping.map_namespace("eiffelevents.jobs"))
        
    def test_init_mapping(self):
        mapping = {
          "eiffelevents_*.allevents": "eiffel000_allevents.documents"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)           
            
        mapping = {
          "eiffelevents.*": "eiffel*_*.documents"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)
        
        mapping = {
          "eiffelevents.allevents": "eiffel000_allevents.documents",
          "testevents.allevents": "eiffel000_allevents.documents"
        }
        self.assertRaises(errors.InvalidConfiguration, self.setup_mapping, [], [], mapping)
        
        mapping = {
          "eiffelevents.*": "eiffel000_*.documents",
          "testevents.a": "test000_a.b"
        }
        self.setup_mapping([], [], mapping)
        self.assertDictEqual(self.mapping.plain, {"testevents.a":"test000_a.b"})
        self.assertDictEqual(self.mapping.plain_db, {"testevents":["test000_a"]})
        self.assertDictEqual(self.mapping.reverse_plain, {"test000_a.b":"testevents.a"})
        self.assertDictEqual(self.mapping.wildcard, {"eiffelevents.*":"eiffel000_*.documents"})
        
    def test_map(self):
        include = ["eiffelevents.*"]
        exclude = ["eiffelevents.jobs"]
        mapping = {
          "eiffelevents.*": "eiffel000_*.documents"
        }
        self.setup_mapping(include, exclude, mapping)
        self.assertDictEqual(self.mapping.plain, {})
        self.assertDictEqual(self.mapping.wildcard, {"eiffelevents.*": "eiffel000_*.documents"})
        
        # when we get a matched map, plain should contain that one afterwards
        self.assertEqual(self.mapping.get("eiffelevents.allevents", "eiffelevents.allevents"), "eiffel000_allevents.documents")
        self.assertDictEqual(self.mapping.plain, {"eiffelevents.allevents": "eiffel000_allevents.documents"})
        
        # when we get a matched map, plain should contain that one afterwards
        self.assertEqual(self.mapping.get("eiffelevents.artifacts", "eiffelevents.artifacts"), "eiffel000_artifacts.documents")
        self.assertDictEqual(self.mapping.plain, {"eiffelevents.allevents": "eiffel000_allevents.documents", "eiffelevents.artifacts": "eiffel000_artifacts.documents"})
        
        self.assertEqual(self.mapping.get_key("eiffel000_allevents.documents"), "eiffelevents.allevents")
        self.assertSetEqual(set(self.mapping.map_db("eiffelevents")),set(["eiffel000_artifacts", "eiffel000_allevents"]))
        self.assertEqual(self.mapping.map_namespace("eiffelevents.allevents"), "eiffel000_allevents.documents")
        
        

if __name__ == '__main__':
    unittest.main()
    