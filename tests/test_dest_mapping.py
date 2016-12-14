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
from mongo_connector.dest_mapping import (
    DestMapping, Namespace, match_replace_regex, namespace_to_regex,
    RegexSet, wildcards_overlap)
from mongo_connector import errors


class TestDestMapping(unittest.TestCase):
    """Test the DestMapping class"""

    def test_default(self):
        """Test that by default, all namespaces are kept without renaming"""
        dest_mapping = DestMapping()
        self.assertEqual(dest_mapping.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.map_db("db1"), ["db1"])
        self.assertEqual(dest_mapping.map_namespace("db1.col1"), "db1.col1")

    def test_include_plain(self):
        """Test including namespaces without wildcards"""
        dest_mapping = DestMapping(namespace_set=["db1.col1", "db1.col2"])
        self.assertEqual(dest_mapping.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.unmap_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(dest_mapping.unmap_namespace("not.included"))
        self.assertEqual(dest_mapping.map_db("db1"), ["db1"])
        self.assertEqual(dest_mapping.map_db("not_included"), [])
        self.assertEqual(dest_mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.map_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(dest_mapping.map_namespace("db1.col4"))

    def test_include_wildcard(self):
        """Test including namespaces with wildcards"""
        equivalent_dest_mappings = (
            DestMapping(namespace_set=["db1.*"]),
            DestMapping(user_mapping={"db1.*": {}}),
            DestMapping(user_mapping={"db1.*": {"rename": "db1.*"}}))
        for dest_mapping in equivalent_dest_mappings:
            self.assertEqual(dest_mapping.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(dest_mapping.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(dest_mapping.lookup("db1.col1"),
                             Namespace(dest_name="db1.col1",
                                       source_name="db1.col1"))
            self.assertListEqual(dest_mapping.map_db("db1"), ["db1"])
            self.assertEqual(dest_mapping.map_namespace("db1.col1"),
                             "db1.col1")
            self.assertIsNone(dest_mapping.map_namespace("db2.col4"))

    def test_map_db_wildcard(self):
        """Test a crazy namespace renaming scheme with wildcards."""
        dest_mapping = DestMapping(user_mapping={
            "db.1_*": "db1.new_*",
            "db.2_*": "db2.new_*",
            "db.3": "new_db.3"})
        self.assertEqual(set(dest_mapping.map_db("db")),
                         set(["db1", "db2", "new_db"]))

    def test_map_db_wildcard_invalid(self):
        """Test mapping a database with wildcards."""
        # This behavior demonstrates why it should be invalid to move a
        # wildcard from the source collection name to the target database
        # name. map_db() should always return all the corresponding database
        # names. Otherwise, the dropDatabase command may not drop all the
        # target databases.
        dest_mapping = DestMapping(user_mapping={"db.*": "db_*.new"})
        self.assertEqual(dest_mapping.map_db("db"), [])
        self.assertEqual(dest_mapping.map_namespace("db.1"), "db_1.new")
        self.assertEqual(dest_mapping.map_db("db"), ["db_1"])
        self.assertEqual(dest_mapping.map_namespace("db.2"), "db_2.new")
        self.assertEqual(set(dest_mapping.map_db("db")),
                         set(["db_1", "db_2"]))

    def test_include_wildcard_periods(self):
        """Test the '.' in the namespace only matches '.'"""
        dest_mapping = DestMapping(namespace_set=["db.*"])
        self.assertIsNone(dest_mapping.map_namespace("dbxcol"))
        self.assertEqual(dest_mapping.map_namespace("db.col"), "db.col")

    def test_include_wildcard_multiple_periods(self):
        """Test matching a namespace with multiple '.' characters."""
        dest_mapping = DestMapping(namespace_set=["db.col.*"])
        self.assertIsNone(dest_mapping.map_namespace("db.col"))
        self.assertEqual(dest_mapping.map_namespace("db.col."), "db.col.")

    def test_include_wildcard_no_period_in_database(self):
        """Test that a database wildcard cannot match a period."""
        dest_mapping = DestMapping(namespace_set=["db*.col"])
        self.assertIsNone(dest_mapping.map_namespace("db.bar.col"))
        self.assertEqual(dest_mapping.map_namespace("dbfoo.col"), "dbfoo.col")

    def test_include_wildcard_metacharacters(self):
        """Test namespaces with metacharacters are matched."""
        dest_mapping = DestMapping(namespace_set=["db&_*.$_^_#_!_[_]_"])
        self.assertEqual(dest_mapping.map_namespace("db&_foo.$_^_#_!_[_]_"),
                         "db&_foo.$_^_#_!_[_]_")
        self.assertIsNone(dest_mapping.map_namespace("db&.foo"))

    def test_exclude_plain(self):
        """Test excluding namespaces without wildcards"""
        dest_mapping = DestMapping(ex_namespace_set=["ex.clude"])
        self.assertEqual(dest_mapping.unmap_namespace("db.col"), "db.col")
        self.assertEqual(dest_mapping.unmap_namespace("ex.clude"), "ex.clude")
        self.assertEqual(dest_mapping.map_namespace("db.col"), "db.col")
        self.assertIsNone(dest_mapping.map_namespace("ex.clude"))

    def test_exclude_wildcard(self):
        """Test excluding namespaces with wildcards"""
        dest_mapping = DestMapping(ex_namespace_set=["ex.*"])
        self.assertEqual(dest_mapping.unmap_namespace("db.col"), "db.col")
        self.assertEqual(dest_mapping.unmap_namespace("ex.clude"), "ex.clude")
        self.assertEqual(dest_mapping.map_namespace("db.col"), "db.col")
        self.assertIsNone(dest_mapping.map_namespace("ex.clude"))
        self.assertIsNone(dest_mapping.map_namespace("ex.clude2"))

    def test_unmap_namespace_wildcard(self):
        """Test un-mapping a namespace that was never explicitly mapped."""
        dest_mapping = DestMapping(user_mapping={
            "db2.*": "db2.f*",
            "db_*.foo": "db_new_*.foo",
        })
        self.assertEqual(dest_mapping.unmap_namespace("db2.foo"), "db2.oo")
        self.assertEqual(dest_mapping.unmap_namespace("db_new_123.foo"),
                         "db_123.foo")

    def test_rename_validation(self):
        """Test namespace renaming validation."""
        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(user_mapping={
                "db1.col1": "newdb.newcol",
                "db2.col1": "newdb.newcol"})

        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(user_mapping={
                "db*.col1": "newdb.newcol*",
                "db*.col2": "newdb.newcol*"})

        # Multiple collections cannot be merged into the same target namespace
        dest_mapping = DestMapping(user_mapping={
            "d*.coll": "d*.coll",
            "db1.c*l": "db.c*l"})
        with self.assertRaises(errors.InvalidConfiguration):
            dest_mapping.map_namespace("db.coll")
            dest_mapping.map_namespace("db1.coll")

        # Multiple collections cannot be merged into the same target namespace
        dest_mapping = DestMapping(user_mapping={
            "*.important_coll": "*.super_important_coll",
            "important_db.*": "super_important_db.*"})
        with self.assertRaises(errors.InvalidConfiguration):
            dest_mapping.map_namespace("super_important_db.important_coll")
            dest_mapping.map_namespace("important_db.super_important_coll")

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
