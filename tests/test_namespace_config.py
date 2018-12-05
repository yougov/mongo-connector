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

import json
import re
import sys

import importlib_resources

from tests import unittest
from mongo_connector.namespace_config import (
    NamespaceConfig,
    Namespace,
    match_replace_regex,
    namespace_to_regex,
    RegexSet,
    wildcards_overlap,
)
from mongo_connector import errors


class TestNamespaceConfig(unittest.TestCase):
    """Test the NamespaceConfig class"""

    def test_default(self):
        """Test that by default, all namespaces are kept without renaming"""
        namespace_config = NamespaceConfig()
        self.assertEqual(namespace_config.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(namespace_config.map_db("db1"), ["db1"])
        self.assertEqual(namespace_config.map_namespace("db1.col1"), "db1.col1")

    def test_config(self):
        """Test that the namespace option in the example config is valid."""
        package = "mongo_connector.service"
        stream = importlib_resources.open_text(package, "config.json")
        with stream:
            namespaces = json.load(stream)["__namespaces"]
        NamespaceConfig(namespace_options=namespaces)

    def test_include_plain(self):
        """Test including namespaces without wildcards"""
        namespace_config = NamespaceConfig(namespace_set=["db1.col1", "db1.col2"])
        self.assertEqual(namespace_config.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(namespace_config.unmap_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(namespace_config.unmap_namespace("not.included"))
        self.assertEqual(namespace_config.map_db("db1"), ["db1"])
        self.assertEqual(namespace_config.map_db("not_included"), [])
        self.assertEqual(namespace_config.map_namespace("db1.col1"), "db1.col1")
        self.assertEqual(namespace_config.map_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(namespace_config.map_namespace("db1.col4"))

    def test_include_wildcard(self):
        """Test including namespaces with wildcards"""
        equivalent_namespace_configs = (
            NamespaceConfig(namespace_set=["db1.*"]),
            NamespaceConfig(namespace_options={"db1.*": {}}),
            NamespaceConfig(namespace_options={"db1.*": True}),
            NamespaceConfig(namespace_options={"db1.*": {"rename": "db1.*"}}),
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(namespace_config.unmap_namespace("db1.col1"), "db1.col1")
            self.assertEqual(namespace_config.unmap_namespace("db1.col2"), "db1.col2")
            self.assertEqual(
                namespace_config.lookup("db1.col1"),
                Namespace(dest_name="db1.col1", source_name="db1.col1"),
            )
            self.assertListEqual(namespace_config.map_db("db1"), ["db1"])
            self.assertEqual(namespace_config.map_namespace("db1.col1"), "db1.col1")
            self.assertIsNone(namespace_config.map_namespace("db2.col4"))

    def test_map_db_wildcard(self):
        """Test a crazy namespace renaming scheme with wildcards."""
        namespace_config = NamespaceConfig(
            namespace_options={
                "db.1_*": "db1.new_*",
                "db.2_*": "db2.new_*",
                "db.3": "new_db.3",
            }
        )
        self.assertEqual(
            set(namespace_config.map_db("db")), set(["db1", "db2", "new_db"])
        )

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
        self.assertEqual(namespace_config.map_namespace("dbfoo.col"), "dbfoo.col")

    def test_include_wildcard_metacharacters(self):
        """Test namespaces with metacharacters are matched."""
        namespace_config = NamespaceConfig(namespace_set=["db&_*.$_^_#_!_[_]_"])
        self.assertEqual(
            namespace_config.map_namespace("db&_foo.$_^_#_!_[_]_"),
            "db&_foo.$_^_#_!_[_]_",
        )
        self.assertIsNone(namespace_config.map_namespace("db&.foo"))

    def test_gridfs(self):
        """Test the gridfs property is set correctly."""
        equivalent_namespace_configs = (
            NamespaceConfig(gridfs_set=["db1.*"]),
            NamespaceConfig(namespace_options={"db1.*": {"gridfs": True}}),
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(namespace_config.unmap_namespace("db1.col1"), "db1.col1")
            self.assertEqual(namespace_config.unmap_namespace("db1.col2"), "db1.col2")
            self.assertEqual(
                namespace_config.lookup("db1.col1"),
                Namespace(dest_name="db1.col1", source_name="db1.col1", gridfs=True),
            )
            self.assertListEqual(namespace_config.map_db("db1"), ["db1"])
            self.assertEqual(namespace_config.map_namespace("db1.col1"), "db1.col1")
            self.assertIsNone(namespace_config.map_namespace("db2.col4"))
            self.assertTrue(namespace_config.lookup("db1.col1").gridfs)
            self.assertEqual(namespace_config.gridfs_namespace("db1.col1"), "db1.col1")
            self.assertIsNone(namespace_config.gridfs_namespace("not.gridfs"))

    def test_exclude_plain(self):
        """Test excluding namespaces without wildcards"""
        namespace_config = NamespaceConfig(ex_namespace_set=["ex.clude"])
        self.assertEqual(namespace_config.unmap_namespace("db.col"), "db.col")
        self.assertEqual(namespace_config.unmap_namespace("ex.clude"), "ex.clude")
        self.assertEqual(namespace_config.map_namespace("db.col"), "db.col")
        self.assertIsNone(namespace_config.map_namespace("ex.clude"))

    def test_exclude_wildcard(self):
        """Test excluding namespaces with wildcards"""
        equivalent_namespace_configs_for_tests = (
            NamespaceConfig(ex_namespace_set=["ex.*", "ex2.*"]),
            NamespaceConfig(namespace_options={"ex.*": False, "ex2.*": False}),
            # Multiple wildcards in exclude namespace
            NamespaceConfig(ex_namespace_set=["e*.*"]),
        )
        for namespace_config in equivalent_namespace_configs_for_tests:
            self.assertEqual(namespace_config.unmap_namespace("db.col"), "db.col")
            self.assertEqual(namespace_config.unmap_namespace("ex.clude"), "ex.clude")
            self.assertEqual(namespace_config.map_namespace("db.col"), "db.col")
            self.assertIsNone(namespace_config.map_namespace("ex.clude"))
            self.assertIsNone(namespace_config.map_namespace("ex2.clude"))

    def test_include_and_exclude(self):
        """Test including and excluding namespaces at the same time."""
        equivalent_namespace_configs_for_tests = (
            NamespaceConfig(
                ex_namespace_set=["ex.*"],
                namespace_set=["ex.cluded_still", "in.cluded"],
            ),
            NamespaceConfig(
                namespace_options={
                    "ex.*": False,
                    "ex.cluded_still": True,
                    "in.cluded": True,
                }
            ),
            NamespaceConfig(
                ex_namespace_set=["ex.cluded", "ex.cluded_still"],
                namespace_set=["ex.*", "in.cluded"],
            ),
        )
        for namespace_config in equivalent_namespace_configs_for_tests:
            self.assertIsNone(namespace_config.map_namespace("ex.cluded"))
            # Excluded namespaces take precedence over included ones.
            self.assertIsNone(namespace_config.map_namespace("ex.cluded_still"))
            # Namespaces that are not explicitly included are ignored.
            self.assertIsNone(namespace_config.map_namespace("also.not.included"))
            self.assertEqual(namespace_config.map_namespace("in.cluded"), "in.cluded")

    def test_include_and_exclude_validation(self):
        """Test including and excluding the same namespaces is an error."""
        equivalent_namespace_config_kwargs = (
            dict(
                ex_namespace_set=["ex.cluded"], namespace_set=["in.cluded", "ex.cluded"]
            ),
            dict(
                namespace_set=["ex.cluded"],
                namespace_options={"ex.cluded": False, "in.cluded": True},
            ),
            dict(
                ex_namespace_set=["ex.cluded", "in.cluded"],
                namespace_options={"in.cluded": True},
            ),
        )
        for kwargs in equivalent_namespace_config_kwargs:
            with self.assertRaises(errors.InvalidConfiguration):
                NamespaceConfig(**kwargs)

    def test_unmap_namespace_wildcard(self):
        """Test un-mapping a namespace that was never explicitly mapped."""
        namespace_config = NamespaceConfig(
            namespace_options={"db2.*": "db2.f*", "db_*.foo": "db_new_*.foo"}
        )
        self.assertEqual(namespace_config.unmap_namespace("db2.foo"), "db2.oo")
        self.assertEqual(
            namespace_config.unmap_namespace("db_new_123.foo"), "db_123.foo"
        )

    def test_override_namespace_options(self):
        """Test gridfs_set and dest_mapping arguments override
        namespace_options.
        """
        namespace_config = NamespaceConfig(
            namespace_set=["override.me", "override.me2"],
            gridfs_set=["override.me3"],
            dest_mapping={
                "override.me": "overridden.1",
                "override.me2": "overridden.2",
            },
            namespace_options={
                "override.me": {
                    "rename": "override.me",
                    "includeFields": ["_id", "dont_remove"],
                },
                "override.me2": "override.me2",
                "override.me3": {"gridfs": False},
            },
        )
        overridden = namespace_config.lookup("override.me")
        self.assertEqual(overridden.dest_name, "overridden.1")
        self.assertEqual(overridden.include_fields, set(["_id", "dont_remove"]))
        overridden = namespace_config.lookup("override.me2")
        self.assertEqual(overridden.dest_name, "overridden.2")
        self.assertFalse(overridden.include_fields)
        self.assertTrue(namespace_config.gridfs_namespace("override.me3"))

    def test_invalid_collection_name_validation(self):
        """Test that invalid collection names raise InvalidConfiguration."""
        equivalent_namespace_config_kwargs = (
            dict(namespace_options={"invalid_db": "newinvalid_db"}),
            dict(namespace_set=["invalid_db."]),
            dict(ex_namespace_set=[".invalid_db"]),
            dict(gridfs_set=[".invalid_db"]),
        )
        for kwargs in equivalent_namespace_config_kwargs:
            with self.assertRaises(errors.InvalidConfiguration):
                NamespaceConfig(**kwargs)

    def test_gridfs_rename_invalid(self):
        """Test that renaming a GridFS collection is invalid."""
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                namespace_options={
                    "gridfs.*": {"rename": "new_gridfs.*", "gridfs": True}
                }
            )

    def test_rename_validation(self):
        """Test namespace renaming validation."""
        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                namespace_options={
                    "db1.col1": "newdb.newcol",
                    "db2.col1": "newdb.newcol",
                }
            )

        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                namespace_options={
                    "db*.col1": "newdb.newcol*",
                    "db*.col2": "newdb.newcol*",
                }
            )

        # Multiple collections cannot be merged into the same target namespace
        namespace_config = NamespaceConfig(
            namespace_options={"*.coll": "*.new_coll", "db.*": "new_db.*"}
        )
        namespace_config.map_namespace("new_db.coll")
        with self.assertRaises(errors.InvalidConfiguration):
            # "db.new_coll" should map to "new_db.new_coll" but there is
            # already a mapping from "new_db.coll" to "new_db.new_coll".
            namespace_config.map_namespace("db.new_coll")

        # For the sake of map_db, wildcards cannot be moved from database name
        # to collection name.
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(namespace_options={"db*.col": "new_db.col_*"})

        # For the sake of map_db, wildcards cannot be moved from collection
        # name to database name.
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(namespace_options={"db.*": "new_db_*.col"})

    def test_fields_validation(self):
        """Test including/excluding fields per namespace."""
        # Cannot include and exclude fields in the same namespace
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                namespace_options={
                    "db.col": {"includeFields": ["a"], "excludeFields": ["b"]}
                }
            )

        # Cannot include fields globally and then exclude fields
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                include_fields=["a"],
                namespace_options={"db.col": {"excludeFields": ["b"]}},
            )

        # Cannot exclude fields globally and then include fields
        with self.assertRaises(errors.InvalidConfiguration):
            NamespaceConfig(
                exclude_fields=["b"],
                namespace_options={"db.col": {"includeFields": ["a"]}},
            )

    def test_projection_include_wildcard(self):
        """Test include_fields on a wildcard namespace."""
        equivalent_namespace_configs = (
            NamespaceConfig(
                include_fields=["foo", "nested.field"],
                ex_namespace_set=["ignored.name"],
            ),
            NamespaceConfig(
                include_fields=["foo", "nested.field"], namespace_set=["db.foo"]
            ),
            NamespaceConfig(
                include_fields=["foo", "nested.field"], namespace_set=["db.*"]
            ),
            NamespaceConfig(
                namespace_options={"db.*": {"includeFields": ["foo", "nested.field"]}}
            ),
            NamespaceConfig(
                namespace_options={"db.foo": {"includeFields": ["foo", "nested.field"]}}
            ),
            NamespaceConfig(
                include_fields=["foo", "nested.field"],
                namespace_options={"db.*": {"includeFields": ["foo", "nested.field"]}},
            ),
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(
                namespace_config.projection("db.foo"),
                {"_id": 1, "foo": 1, "nested.field": 1},
            )
            self.assertIsNone(namespace_config.projection("ignored.name"))

    def test_projection_exclude_wildcard(self):
        """Test exclude_fields on a wildcard namespace."""
        equivalent_namespace_configs = (
            NamespaceConfig(
                exclude_fields=["_id", "foo", "nested.field"], namespace_set=["db.*"]
            ),
            NamespaceConfig(
                namespace_options={
                    "db.*": {"excludeFields": ["_id", "foo", "nested.field"]}
                }
            ),
            NamespaceConfig(
                exclude_fields=["foo", "nested.field"],
                namespace_options={
                    "db.*": {"excludeFields": ["_id", "foo", "nested.field"]}
                },
            ),
        )
        for namespace_config in equivalent_namespace_configs:
            self.assertEqual(
                namespace_config.projection("db.foo"), {"foo": 0, "nested.field": 0}
            )
            self.assertIsNone(namespace_config.projection("ignored.name"))

    def test_get_included_databases(self):
        """Test get_included_databases."""
        nc_requires_list_databases = (
            NamespaceConfig(),
            NamespaceConfig(namespace_options={"db.c": False}),
            NamespaceConfig(namespace_options={"db*.c": True}),
        )
        for namespace_config in nc_requires_list_databases:
            self.assertEqual(namespace_config.get_included_databases(), [])

        namespace_config = NamespaceConfig(namespace_options={"db.c": True})
        self.assertEqual(namespace_config.get_included_databases(), ["db"])
        namespace_config = NamespaceConfig(namespace_options={"db.*": True})
        self.assertEqual(namespace_config.get_included_databases(), ["db"])

    def test_match_replace_regex(self):
        """Test regex matching and replacing."""
        regex = re.compile(r"\Adb_([^.]*).foo\Z")
        self.assertIsNone(match_replace_regex(regex, "db.foo", "*.foo"))
        self.assertIsNone(match_replace_regex(regex, "db.foo.foo", "*.foo"))
        self.assertEqual(match_replace_regex(regex, "db_bar.foo", "*.foo"), "bar.foo")

    def test_namespace_to_regex(self):
        """Test regex creation."""
        self.assertEqual(
            namespace_to_regex("db*.foo"), re.compile(r"\Adb([^.]*)\.foo\Z")
        )
        self.assertEqual(namespace_to_regex("db.foo*"), re.compile(r"\Adb\.foo(.*)\Z"))
        self.assertEqual(namespace_to_regex("db.foo"), re.compile(r"\Adb\.foo\Z"))

    @unittest.skipIf(sys.version_info >= (3, 7), "Python 3.6 and earlier")
    def test_namespace_to_regex_escapes_metacharacters_legacy(self):
        """Test regex creation escapes metacharacters."""
        self.assertEqual(
            namespace_to_regex("db&*.$a^a#a!a[a]a"),
            re.compile(r"\Adb\&([^.]*)\.\$a\^a\#a\!a\[a\]a\Z"),
        )
        self.assertEqual(
            namespace_to_regex("db.$a^a#a!a[a]a*"),
            re.compile(r"\Adb\.\$a\^a\#a\!a\[a\]a(.*)\Z"),
        )

    @unittest.skipIf(sys.version_info < (3, 7), "Python 3.7 and later")
    def test_namespace_to_regex_escapes_metacharacters(self):
        """Test regex creation escapes metacharacters."""
        self.assertEqual(
            namespace_to_regex("db&*.$a^a#a!a[a]a"),
            re.compile(r"\Adb\&([^.]*)\.\$a\^a\#a!a\[a\]a\Z"),
        )
        self.assertEqual(
            namespace_to_regex("db.$a^a#a!a[a]a*"),
            re.compile(r"\Adb\.\$a\^a\#a!a\[a\]a(.*)\Z"),
        )

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
        self.assertEqual(RegexSet.from_namespaces([]), RegexSet([], []))
        self.assertEqual(
            RegexSet.from_namespaces(["db.bar", "db_*.foo", "db2.*"]),
            RegexSet(
                [namespace_to_regex("db_*.foo"), namespace_to_regex("db2.*")],
                ["db.bar"],
            ),
        )

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
