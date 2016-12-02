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

import logging
import threading
import re

from collections import namedtuple, MutableSet

from mongo_connector import errors


LOG = logging.getLogger(__name__)


_MappedNamespace = namedtuple('MappedNamespace',
                              ['name', 'include_fields', 'exclude_fields'])


class MappedNamespace(_MappedNamespace):
    def __new__(cls, name=None, include_fields=None, exclude_fields=None):
        return super(MappedNamespace, cls).__new__(
            cls, name, include_fields, exclude_fields)


class RegexSet(MutableSet):
    """Set that stores both plain strings and RegexObjects.

    Membership query results are cached so that repeated lookups of the same
    string are fast.
    """
    def __init__(self, regexes, strings):
        self._regexes = set(regexes)
        self._plain = set(strings)
        self._not_found_cache = set()

    def __contains__(self, item):
        if item in self._not_found_cache:
            return False
        if item in self._plain:
            return True
        if item in self._regexes:
            return True
        for regex in self._regexes:
            if regex.match(item):
                self._plain.add(item)
                return True
        self._not_found_cache.add(item)
        return False

    def __iter__(self):
        for regex in self._regexes:
            yield regex
        for string in self._plain:
            yield string

    def __len__(self):
        return len(self._regexes) + len(self._plain)

    def add(self, string):
        self._plain.add(string)
        self._not_found_cache.discard(string)

    def discard(self, string):
        self._plain.discard(string)

    @staticmethod
    def from_namespaces(namespaces):
        regexes = set()
        strings = set()
        for ns in namespaces:
            if '*' in ns:
                regexes.add(namespace_to_regex(ns))
            else:
                strings.add(ns)
        return RegexSet(regexes, strings)


class DestMapping(object):
    """Manages included and excluded namespaces.
    """
    def __init__(self, namespace_set=None, ex_namespace_set=None,
                 user_mapping=None, include_fields=None,
                 exclude_fields=None):
        # a dict containing plain mappings
        self.plain = {}
        # a list of (RegexObject, original namespace, MappedNamespace) tuples
        self.regex_map = []
        # a dict containing reverse plain mapping
        # because the mappings are not duplicated,
        # so the values should also be unique
        self.reverse_plain = {}
        # a dict containing plain db mappings, db -> a set of mapped db
        self.plain_db = {}

        # Fields to include or exclude from all namespaces
        self.include_fields = include_fields
        self.exclude_fields = exclude_fields

        # the input namespace_set and ex_namespace_set could contain wildcard
        self.namespace_set = set()
        self.ex_namespace_set = RegexSet.from_namespaces(
            ex_namespace_set or [])

        self.lock = threading.Lock()

        user_mapping = user_mapping or {}
        namespace_set = namespace_set or []
        # initialize
        for ns in namespace_set:
            user_mapping.setdefault(ns, ns)

        for src_name, v in user_mapping.items():
            if isinstance(v, dict):
                self._add_mapping(src_name, v.get('rename'))
            else:
                self._add_mapping(src_name, v)

    def _add_mapping(self, src_name, dest_name=None, include_fields=None,
                     exclude_fields=None):
        if (self.include_fields and exclude_fields or
                self.exclude_fields and include_fields or
                include_fields and exclude_fields):
            raise errors.InvalidConfiguration(
                "Cannot mix include fields and exclude fields in "
                "namespace mapping for: '%s'" % (src_name,))
        if dest_name is None:
            dest_name = src_name
        self.set(src_name, MappedNamespace(dest_name, include_fields,
                                           exclude_fields))
        # Add the namespace for commands on this database
        cmd_name = src_name.split('.', 1)[0] + '.$cmd'
        dest_cmd_name = dest_name.split('.', 1)[0] + '.$cmd'
        self.set(cmd_name, MappedNamespace(dest_cmd_name))

    def set_plain(self, src_name, mapped_namespace):
        """A utility function to set the corresponding plain variables"""
        # The lock is necessary to properly check that multiple namespaces
        # into a single namespace.
        with self.lock:
            target_name = mapped_namespace.name
            existing_src = self.reverse_plain.get(target_name)
            if existing_src and existing_src != src_name:
                raise errors.InvalidConfiguration(
                    "Multiple namespaces cannot be combined into one target "
                    "namespace. Trying to map '%s' to '%s' but there already "
                    "exists a mapping from '%s' to '%s'" %
                    (src_name, target_name, existing_src, target_name))

            self.plain[src_name] = mapped_namespace
            self.reverse_plain[target_name] = src_name
            src_db, _ = src_name.split(".", 1)
            target_db, _ = target_name.split(".", 1)
            self.plain_db.setdefault(src_db, set()).add(target_db)

    def get(self, plain_src_ns):
        """Given a plain source namespace, return a mapped namespace if it
        should be included or None.
        """
        # if plain_src_ns matches ex_namespace_set, ignore
        if plain_src_ns in self.ex_namespace_set:
            return None
        if not self.regex_map and not self.plain:
            # here we include all namespaces
            return MappedNamespace(plain_src_ns)
        # search in plain mappings first
        try:
            return self.plain[plain_src_ns]
        except KeyError:
            # search in wildcard mappings
            # if matched, get a replaced mapped namespace
            # and add to the plain mappings
            for regex, _, mapped in self.regex_map:
                new_name = match_replace_regex(regex, plain_src_ns,
                                               mapped.name)
                if not new_name:
                    continue
                new_mapped = MappedNamespace(new_name,
                                             mapped.include_fields,
                                             mapped.exclude_fields)
                self.set_plain(plain_src_ns, new_mapped)
                return new_mapped

        self.ex_namespace_set.add(plain_src_ns)
        return None

    def set(self, src_name, mapped_namespace):
        """Add a new namespace mapping."""
        if "*" in src_name:
            self.regex_map.append((namespace_to_regex(src_name),
                                   src_name, mapped_namespace))
        else:
            self.set_plain(src_name, mapped_namespace)
        self.namespace_set.add(src_name)

    def unmap_namespace(self, plain_mapped_ns):
        """Given a plain mapped namespace, return a source namespace if
        matched. It is possible for the mapped namespace to not yet be present
        in the plain/reverse_plain dictionaries so we search the wildcard
        dictionary as well.
        """
        if not self.regex_map and not self.plain:
            return plain_mapped_ns

        src_name = self.reverse_plain.get(plain_mapped_ns)
        if src_name:
            return src_name
        for _, wildcard_ns, mapped in self.regex_map:
            original_name = match_replace_regex(
                namespace_to_regex(mapped.name), plain_mapped_ns, wildcard_ns)
            if original_name:
                return original_name
        return None

    def map_namespace(self, plain_src_ns):
        """Applies the plain source namespace mapping to a "db.collection" string.
        The input parameter ns is plain text.
        """
        mapped = self.get(plain_src_ns)
        if mapped:
            return mapped.name
        else:
            return None

    def map_db(self, plain_src_db):
        """Applies the namespace mapping to a database.
        Individual collections in a database can be mapped to
        different target databases, so map_db can return multiple results.
        The input parameter db is plain text.
        This is used to dropDatabase, so we assume before drop, those target
        databases should exist and already been put to plain_db when doing
        create/insert operation.
        """
        if not self.regex_map and not self.plain:
            return [plain_src_db]
        # Lookup this namespace to seed the plain_db dictionary
        self.get(plain_src_db + '.$cmd')
        return list(self.plain_db.get(plain_src_db, set()))

    def fields(self, plain_src_ns):
        """Get the fields to include and exclude for a given namespace."""
        mapped = self.get(plain_src_ns)
        if mapped:
            return mapped.include_fields, mapped.exclude_fields
        else:
            return None, None

    def projection(self, plain_src_name, projection):
        """For the given source namespace return the projected fields."""
        include_fields, exclude_fields = self.fields(plain_src_name)
        fields = include_fields or exclude_fields
        include = 1 if include_fields else 0
        if fields:
            full_projection = dict((field, include) for field in fields)
            if projection:
                full_projection.update(projection)
            return full_projection
        return projection


def match_replace_regex(regex, src_namespace, dest_namespace):
    """Return the new mapped namespace if the src_namespace matches the
    regex."""
    match = regex.match(src_namespace)
    if match:
        return dest_namespace.replace('*', match.group(1))
    return None


def namespace_to_regex(namespace):
    """Create a RegexObject from a wildcard namespace."""
    if namespace.find('*') < namespace.find('.'):
        # A database name cannot contain a '.' character
        wildcard_group = '([^.]*)'
    else:
        wildcard_group = '(.*)'
    return re.compile(r'\A' + namespace.replace('*', wildcard_group) + r'\Z')
