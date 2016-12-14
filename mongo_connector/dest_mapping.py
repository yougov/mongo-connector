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
from itertools import combinations

from mongo_connector import errors


LOG = logging.getLogger(__name__)


_MappedNamespace = namedtuple(
    'MappedNamespace',
    ['dest_name', 'source_name', 'include_fields', 'exclude_fields'])


class MappedNamespace(_MappedNamespace):
    def __new__(cls, dest_name=None, source_name=None, include_fields=None,
                exclude_fields=None):
        return super(MappedNamespace, cls).__new__(
            cls, dest_name, source_name, include_fields, exclude_fields)


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
        # A mapping from non-wildcard source namespaces to a MappedNamespace
        # containing the non-wildcard target name.
        self.plain = {}
        # A mapping from non-wildcard target namespaces to their
        # corresponding non-wildcard source namespace. Namespaces have a
        # one-to-one relationship with the target system, meaning multiple
        # source namespaces cannot be merged into a single namespace in the
        # target.
        self.reverse_plain = {}
        # A mapping from non-wildcard source database names to the set of
        # non-wildcard target database names.
        self.plain_db = {}
        # A list of (re.RegexObject, MappedNamespace) tuples. regex_map maps
        # wildcard source namespaces to a MappedNamespace containing the
        # wildcard target name. When a namespace is matched, an entry is
        # created in `self.plain` for faster subsequent lookups.
        self.regex_map = []

        # Fields to include or exclude from all namespaces
        self.include_fields = include_fields
        self.exclude_fields = exclude_fields

        # namespace_set and ex_namespace_set can contain wildcards
        self.namespace_set = set()
        self.ex_namespace_set = RegexSet.from_namespaces(
            ex_namespace_set or [])

        self.lock = threading.Lock()

        user_mapping = user_mapping or {}
        namespace_set = namespace_set or []
        # Add each namespace from the namespace_set and user_mapping
        # parameters. Namespaces have a one-to-one relationship with the
        # target system, meaning multiple source namespaces cannot be merged
        # into a single namespace in the target
        for ns in namespace_set:
            user_mapping.setdefault(ns, ns)

        renames = {}
        for src_name, v in user_mapping.items():
            if isinstance(v, dict):
                target_name = v.get('rename')
            else:
                target_name = v
            renames[src_name] = target_name
            self._add_mapping(src_name, target_name)
        validate_target_namespaces(renames)

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
        self.set(MappedNamespace(dest_name=dest_name, source_name=src_name,
                                 include_fields=include_fields,
                                 exclude_fields=exclude_fields))
        # Add the namespace for commands on this database
        cmd_name = src_name.split('.', 1)[0] + '.$cmd'
        dest_cmd_name = dest_name.split('.', 1)[0] + '.$cmd'
        self.set(MappedNamespace(dest_name=dest_cmd_name, source_name=cmd_name))

    def set_plain(self, mapped_namespace):
        """A utility function to set the corresponding plain variables"""
        # The lock is necessary to properly check that multiple namespaces
        # into a single namespace.
        with self.lock:
            src_name = mapped_namespace.source_name
            target_name = mapped_namespace.dest_name
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
            for regex, mapped in self.regex_map:
                new_name = match_replace_regex(regex, plain_src_ns,
                                               mapped.dest_name)
                if not new_name:
                    continue
                new_mapped = MappedNamespace(
                    dest_name=new_name, source_name=plain_src_ns,
                    include_fields=mapped.include_fields,
                    exclude_fields=mapped.exclude_fields)
                self.set_plain(new_mapped)
                return new_mapped

        self.ex_namespace_set.add(plain_src_ns)
        return None

    def set(self, mapped_namespace):
        """Add a new namespace mapping."""
        src_name = mapped_namespace.source_name
        if "*" in src_name:
            self.regex_map.append((namespace_to_regex(src_name),
                                   mapped_namespace))
        else:
            self.set_plain(mapped_namespace)
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
        for _, mapped in self.regex_map:
            original_name = match_replace_regex(
                namespace_to_regex(mapped.dest_name), plain_mapped_ns,
                mapped.source_name)
            if original_name:
                return original_name
        return None

    def map_namespace(self, plain_src_ns):
        """Applies the plain source namespace mapping to a "db.col" string.
        The input parameter ns is plain text.
        """
        mapped = self.get(plain_src_ns)
        if mapped:
            return mapped.dest_name
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


def _character_matches(name1, name2):
    """Yield the number of characters that match the beginning of each string.
    """
    if name1[0] == '*':
        for i in range(len(name2) + 1):
            yield 1, i
    if name2[0] == '*':
        for i in range(len(name1) + 1):
            yield i, 1
    if name1[0] == name2[0]:
        yield 1, 1


def wildcards_overlap(name1, name2):
    """Return true if two wildcard patterns can match the same string."""
    if not name1 and not name2:
        return True
    if not name1 or not name2:
        return False
    for matched1, matched2 in _character_matches(name1, name2):
        if wildcards_overlap(name1[matched1:], name2[matched2:]):
            return True
    return False


def validate_target_namespaces(user_mapping):
    """Validate that no target namespaces overlap exactly with each other.

    Also warns when wildcard namespaces have a chance of overlapping.
    """
    for namespace1, namespace2 in combinations(user_mapping.keys(), 2):
        if wildcards_overlap(namespace1, namespace2):
            LOG.warn('Namespaces "%s" and "%s" may match the '
                     'same source namespace.', namespace1, namespace2)
        target1 = user_mapping[namespace1]
        target2 = user_mapping[namespace2]
        if target1 == target2:
            raise errors.InvalidConfiguration(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Trying to map '%s' to '%s' but '%s' already "
                "corresponds to '%s' in the target system." %
                (namespace2, target2, namespace1, target1))
        if wildcards_overlap(target1, target2):
            LOG.warn("Multiple namespaces cannot be combined into one target "
                     "namespace. Mapping from '%s' to '%s' might overlap "
                     "with mapping from '%s' to '%s'." %
                     (namespace2, target2, namespace1, target1))


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
    return re.compile(r'\A' +
                      re.escape(namespace).replace('\*', wildcard_group) +
                      r'\Z')
