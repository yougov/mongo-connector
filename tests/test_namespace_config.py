# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

from tests import unittest
from mongo_connector.namespace_config import (
    NamespaceConfig, Namespace, match_replace_regex, namespace_to_regex,
    RegexSet, wildcards_overlap)
from mongo_connector import errors


class TestDNamespaceConfig(unittest.TestCase):
    """Test the NamespaceConfig class"""

    def test_default(self):
        """Test that by default, all namespaces are kept without renaming"""
        namespace_config = NamespaceConfig()
        self.assertEqual(namespace_config.unmap_namespace("db1.col1"),
                         "db1.col1")
        self.assertEqual(namespace_config.map_db("db1"), ["db1"])
        self.assertEqual(namespace_config.map_namespace("db1.col1"),
                         "db1.col1")

    def test_include_plain(self):
        """Test including namespaces without wildcards"""
        namespace_config = NamespaceConfig(
            namespace_set=["db1.col1", "db1.col2"])
        self.assertEqual(namespace_config.unmap_namespace("db1.col1"),
                         "db1.col1")
        self.assertEqual(namespace_config.unmap_namespace("db1.col2"),
                         "db1.col2")
        self.assertIsNone(namespace_config.unmap_namespace("not.included"))
        self.assertEqual(namespace_config.map_db("db1"), ["db1"])
        self.assertEqual(namespace_config.map_db("not_included"), [])
        self.assertEqual(namespace_config.map_namespace("db1.col1"),
                         "db1.col1")
        self.assertEqual(namespace_config.map_namespace("db1.col2"),
                         "db1.col2")
        self.assertIsNone(namespace_config.map_namespace("db1.col4"))

    def test_include_wildcard(self):
        """Test including namespaces with wildcards"""
        equivalent_namespace_configs = (
            NamespaceConfig(namespace_set=["db1.*"]),
            NamespaceConfig(user_mapping={"db1.*": {}}),
            NamespaceConfig(user_mapping={"db1.*": {"rename": "db1.*"}}))
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(namespace_config.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(namespace_config.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(namespace_config.lookup("db1.col1"),
                             Namespace(dest_name="db1.col1",
                                       source_name="db1.col1"))
            self.assertListEqual(namespace_config.map_db("db1"), ["db1"])
            self.assertEqual(namespace_config.map_namespace("db1.col1"),
                             "db1.col1")
            self.assertIsNone(namespace_config.map_namespace("db2.col4"))

    def test_map_db_wildcard(self):
        """Test a crazy namespace renaming scheme with wildcards."""
        namespace_config = NamespaceConfig(user_mapping={
            "db.1_*": "db1.new_*",
            "db.2_*": "db2.new_*",
            "db.3": "new_db.3"})
        self.assertEqual(set(namespace_config.map_db("db")),
                         set(["db1", "db2", "new_db"]))

    def test_include_wildcard_periods(self):
        """Test the '.' in the namespace only matches '.'"""
        namespace_config = NamespaceConfig(namespace_set=["db.*"])
        self.assertIsNone(namespace_config.map_namespace("dbxcol"))
        self.assertEqual(namespace_config.map_namespace("db.col"), "db.col")

    def test_include_wildcard_multiple_periods(self):
        """Test matching a namespace with multiple '.' characters."""
        namespace_config = NamespaceConfig(namespace_set=["db.col.*"])
        self.assertIsNone(namespace_config.map_namespace("db.col"))
        self.assertEqual(namespace_config.map_namespace("db.col."), "db.col.")

    def test_include_wildcard_no_period_in_database(self):
        """Test that a database wildcard cannot match a period."""
        namespace_config = NamespaceConfig(namespace_set=["db*.col"])
        self.assertIsNone(namespace_config.map_namespace("db.bar.col"))
        self.assertEqual(namespace_config.map_namespace("dbfoo.col"),
                         "dbfoo.col")

    def test_include_wildcard_metacharacters(self):
        """Test namespaces with metacharacters are matched."""
        namespace_config = NamespaceConfig(
            namespace_set=["db&_*.$_^_#_!_[_]_"])
        self.assertEqual(
            namespace_config.map_namespace("db&_foo.$_^_#_!_[_]_"),
            "db&_foo.$_^_#_!_[_]_")
        self.assertIsNone(namespace_config.map_namespace("db&.foo"))

    def test_exclude_plain(self):
        """Test excluding namespaces without wildcards"""
        namespace_config = NamespaceConfig(ex_namespace_set=["ex.clude"])
        self.assertEqual(namespace_config.unmap_namespace("db.col"), "db.col")
        self.assertEqual(namespace_config.unmap_namespace("ex.clude"),
                         "ex.clude")
        self.assertEqual(namespace_config.map_namespace("db.col"), "db.col")
        self.assertIsNone(namespace_config.map_namespace("ex.clude"))

    def test_exclude_wildcard(self):
        """Test excluding namespaces with wildcards"""
        namespace_config = NamespaceConfig(ex_namespace_set=["ex.*"])
        self.assertEqual(namespace_config.unmap_namespace("db.col"), "db.col")
        self.assertEqual(namespace_config.unmap_namespace("ex.clude"),
                         "ex.clude")
        self.assertEqual(namespace_config.map_namespace("db.col"), "db.col")
        self.assertIsNone(namespace_config.map_namespace("ex.clude"))
        self.assertIsNone(namespace_config.map_namespace("ex.clude2"))

    def test_unmap_namespace_wildcard(self):
        """Test un-mapping a namespace that was never explicitly mapped."""
        namespace_config = NamespaceConfig(user_mapping={
            "db2.*": "db2.f*",
            "db_*.foo": "db_new_*.foo",
        })
        self.assertEqual(namespace_config.unmap_namespace("db2.foo"), "db2.oo")
        self.assertEqual(namespace_config.unmap_namespace("db_new_123.foo"),
                         "db_123.foo")

    def test_rename_validation(self):
        """Test namespace renaming validation."""
        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(user_mapping={
                "db1.col1": "newdb.newcol",
                "db2.col1": "newdb.newcol"})

        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(user_mapping={
                "db*.col1": "newdb.newcol*",
                "db*.col2": "newdb.newcol*"})

        # Multiple collections cannot be merged into the same target namespace
        namespace_config = NamespaceConfig(user_mapping={
            "*.coll": "*.new_coll",
            "db.*": "new_db.*"})
        namespace_config.map_namespace("new_db.coll")
        with self.assertRaises(errors.InvalidConfiguration):
            # "db.new_coll" should map to "new_db.new_coll" but there is
            # already a mapping from "new_db.coll" to "new_db.new_coll".
            namespace_config.map_namespace("db.new_coll")

        # For the sake of map_db, wildcards cannot be moved from database name
        # to collection name.
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(user_mapping={"db*.col": "new_db.col_*"})

        # For the sake of map_db, wildcards cannot be moved from collection
        # name to database name.
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(user_mapping={"db.*": "new_db_*.col"})

    def test_fields_validation(self):
        """Test including/excluding fields per namespace."""
        # Cannot include and exclude fields in the same namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(user_mapping={
                "db.col": {"fields": ["a"], "excludeFields": ["b"]}})

        # Cannot include fields globally and then exclude fields
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(include_fields=["a"], user_mapping={
                "db.col": {"excludeFields": ["b"]}})

        # Cannot exclude fields globally and then include fields
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(exclude_fields=["b"], user_mapping={
                "db.col": {"fields": ["a"]}})

    def test_projection_include_wildcard(self):
        """Test include_fields on a wildcard namespace."""
        equivalent_namespace_configs = (
            NamespaceConfig(include_fields=["foo", "nested.field"],
                            ex_namespace_set=["ignored.name"]),
            NamespaceConfig(include_fields=["foo", "nested.field"],
                            namespace_set=["db.foo"]),
            NamespaceConfig(include_fields=["foo", "nested.field"],
                            namespace_set=["db.*"]),
            NamespaceConfig(user_mapping={
                "db.*": {"fields": ["foo", "nested.field"]}}),
            NamespaceConfig(user_mapping={
                "db.foo": {"fields": ["foo", "nested.field"]}}),
            NamespaceConfig(include_fields=["foo", "nested.field"],
                            user_mapping={
                                "db.*": {"fields": ["foo", "nested.field"]}})
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(namespace_config.projection("db.foo"),
                             {"_id": 1, "foo": 1, "nested.field": 1})
            self.assertIsNone(namespace_config.projection("ignored.name"))

    def test_projection_exclude_wildcard(self):
        """Test exclude_fields on a wildcard namespace."""
        equivalent_namespace_configs = (
            NamespaceConfig(exclude_fields=["_id", "foo", "nested.field"],
                            namespace_set=["db.*"]),
            NamespaceConfig(user_mapping={
                "db.*": {"excludeFields": ["_id", "foo", "nested.field"]}}),
            NamespaceConfig(
                exclude_fields=["foo", "nested.field"],
                user_mapping={"db.*": {
                    "excludeFields": ["_id", "foo", "nested.field"]}})
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(namespace_config.projection("db.foo"),
                             {"foo": 0, "nested.field": 0})
            self.assertIsNone(namespace_config.projection("ignored.name"))

    def test_match_replace_regex(self):
        """Test regex matching and replacing."""
        regex = re.compile(r"\Adb_([^.]*).foo\Z")
        self.assertIsNone(match_replace_regex(regex, "db.foo", "*.foo"))
        self.assertIsNone(match_replace_regex(regex, "db.foo.foo", "*.foo"))
        self.assertEqual(match_replace_regex(regex, "db_bar.foo", "*.foo"),
                         "bar.foo")

    def test_namespace_to_regex(self):
        """Test regex creation."""
        self.assertEqual(namespace_to_regex("db*.foo"),
                         re.compile(r"\Adb([^.]*)\.foo\Z"))
        self.assertEqual(namespace_to_regex("db.foo*"),
                         re.compile(r"\Adb\.foo(.*)\Z"))
        self.assertEqual(namespace_to_regex("db.foo"),
                         re.compile(r"\Adb\.foo\Z"))

    def test_namespace_to_regex_escapes_metacharacters(self):
        """Test regex creation escapes metacharacters."""
        self.assertEqual(namespace_to_regex("db&*.$a^a#a!a[a]a"),
                         re.compile(r"\Adb\&([^.]*)\.\$a\^a\#a\!a\[a\]a\Z"))
        self.assertEqual(namespace_to_regex("db.$a^a#a!a[a]a*"),
                         re.compile(r"\Adb\.\$a\^a\#a\!a\[a\]a(.*)\Z"))

    def test_wildcards_overlap(self):
        """Test wildcard strings """
        self.assertTrue(wildcards_overlap("foo.bar", "foo.bar"))
        self.assertTrue(wildcards_overlap("f*.bar", "foo.bar"))
        self.assertTrue(wildcards_overlap("*.bar", "foo.*"))
        self.assertTrue(wildcards_overlap("f*.bar", "foo.b*"))
        self.assertTrue(wildcards_overlap("a*b*c", "*c*"))
        self.assertFalse(wildcards_overlap("foo.bar", "foo.bar2"))
        self.assertFalse(wildcards_overlap("foo.*2", "foo.*1"))
        self.assertFalse(wildcards_overlap("foo.*", "food.*"))
        self.assertFalse(wildcards_overlap("a*b*c", "*c*d"))


class TestRegexSet(unittest.TestCase):
    """Test the RegexSet class."""

    def test_from_namespaces(self):
        """Test construction from list of namespaces."""
        self.assertEqual(RegexSet.from_namespaces([]),
                         RegexSet([], []))
        self.assertEqual(RegexSet.from_namespaces(["db.bar", "db_*.foo",
                                                   "db2.*"]),
                         RegexSet([namespace_to_regex("db_*.foo"),
                                   namespace_to_regex("db2.*")],
                                  ["db.bar"]))

    def test_contains(self):
        """Test membership query."""
        regex_set = RegexSet.from_namespaces(["db.bar", "db_*.foo", "db2.*"])
        self.assertTrue("db.bar" in regex_set)
        self.assertTrue("db_1.foo" in regex_set)
        self.assertTrue("db2.bar" in regex_set)
        self.assertTrue("db2.bar2" in regex_set)
        self.assertFalse("not.found" in regex_set)
        self.assertFalse("db_not.found" in regex_set)

    def test_add(self):
        """Test adding a new string."""
        regex_set = RegexSet.from_namespaces([])
        self.assertFalse("string.found" in regex_set)
        regex_set.add("string.found")
        self.assertTrue("string.found" in regex_set)

    def test_discard(self):
        """Test discarding a string."""
        regex_set = RegexSet.from_namespaces(["db.bar"])
        self.assertTrue("db.bar" in regex_set)
        regex_set.discard("db.bar")
        self.assertFalse("db.bar" in regex_set)

if __name__ == "__main__":
    unittest.main()
