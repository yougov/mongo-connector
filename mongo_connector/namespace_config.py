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
import re

from collections import namedtuple, MutableSet
from itertools import combinations

from mongo_connector import errors


LOG = logging.getLogger(__name__)


_Namespace = namedtuple('Namespace', ['dest_name', 'source_name',
                                      'include_fields', 'exclude_fields'])


class Namespace(_Namespace):
    def __new__(cls, dest_name=None, source_name=None, include_fields=None,
                exclude_fields=None):
        include_fields = set(include_fields or [])
        exclude_fields = set(exclude_fields or [])
        return super(Namespace, cls).__new__(
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


class NamespaceConfig(object):
    """Manages included and excluded namespaces.
    """
    def __init__(self, namespace_set=None, ex_namespace_set=None,
                 user_mapping=None, include_fields=None, exclude_fields=None):
        # A mapping from non-wildcard source namespaces to a MappedNamespace
        # containing the non-wildcard target name.
        self._plain = {}
        # A mapping from non-wildcard target namespaces to their
        # corresponding non-wildcard source namespace. Namespaces have a
        # one-to-one relationship with the target system, meaning multiple
        # source namespaces cannot be merged into a single namespace in the
        # target.
        self._reverse_plain = {}
        # A mapping from non-wildcard source database names to the set of
        # non-wildcard target database names.
        self._plain_db = {}
        # A list of (re.RegexObject, MappedNamespace) tuples. regex_map maps
        # wildcard source namespaces to a MappedNamespace containing the
        # wildcard target name. When a namespace is matched, an entry is
        # created in `self.plain` for faster subsequent lookups.
        self._regex_map = []

        # Fields to include or exclude from all namespaces
        self._include_fields = validate_include_fields(include_fields)
        self._exclude_fields = validate_exclude_fields(exclude_fields)

        # The set of namespaces to exclude. Can contain wildcard namespaces.
        self._ex_namespace_set = RegexSet.from_namespaces(
            ex_namespace_set or [])

        user_mapping = user_mapping or {}
        namespace_set = namespace_set or []
        # Add each namespace from the namespace_set and user_mapping
        # parameters. Namespaces have a one-to-one relationship with the
        # target system, meaning multiple source namespaces cannot be merged
        # into a single namespace in the target.
        for ns in namespace_set:
            user_mapping.setdefault(ns, ns)

        renames = {}
        for src_name, v in user_mapping.items():
            include_fields, exclude_fields = None, None
            if isinstance(v, dict):
                target_name = v.get('rename', src_name)
                include_fields = v.get('fields')
                exclude_fields = v.get('excludeFields')
            else:
                target_name = v
            renames[src_name] = target_name
            self._add_collection(src_name, target_name, include_fields,
                                 exclude_fields)
        validate_target_namespaces(renames)

    def _add_collection(self, src_name, dest_name, include_fields,
                        exclude_fields):
        """Add the collection name and the corresponding command namespace."""
        include_fields = include_fields or []
        exclude_fields = exclude_fields or []
        if (self._include_fields and exclude_fields or
                self._exclude_fields and include_fields or
                include_fields and exclude_fields):
            raise errors.InvalidConfiguration(
                "Cannot mix include fields and exclude fields in "
                "namespace mapping for: '%s'" % (src_name,))
        if dest_name is None:
            dest_name = src_name
        self._add_namespace(Namespace(
            dest_name=dest_name, source_name=src_name,
            include_fields=validate_include_fields(self._include_fields,
                                                   include_fields),
            exclude_fields=validate_exclude_fields(self._exclude_fields,
                                                   exclude_fields)))
        # Add the namespace for commands on this database
        cmd_name = src_name.split('.', 1)[0] + '.$cmd'
        dest_cmd_name = dest_name.split('.', 1)[0] + '.$cmd'
        self._add_namespace(Namespace(
            dest_name=dest_cmd_name, source_name=cmd_name))

    def _add_namespace(self, mapped_namespace):
        """Add an included and possibly renamed Namespace."""
        src_name = mapped_namespace.source_name
        if "*" in src_name:
            self._regex_map.append((namespace_to_regex(src_name),
                                    mapped_namespace))
        else:
            self._add_plain_namespace(mapped_namespace)

    def _add_plain_namespace(self, mapped_namespace):
        """Add an included and possibly renamed non-wildcard Namespace."""
        src_name = mapped_namespace.source_name
        target_name = mapped_namespace.dest_name
        src_names = self._reverse_plain.setdefault(target_name, set())
        src_names.add(src_name)
        if len(src_names) > 1:
            # Another source namespace is already mapped to this target
            existing_src = (src_names - set([src_name])).pop()
            raise errors.InvalidConfiguration(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Trying to map '%s' to '%s' but there already "
                "exists a mapping from '%s' to '%s'" %
                (src_name, target_name, existing_src, target_name))

        self._plain[src_name] = mapped_namespace
        src_db, _ = src_name.split(".", 1)
        target_db, _ = target_name.split(".", 1)
        self._plain_db.setdefault(src_db, set()).add(target_db)

    def lookup(self, plain_src_ns):
        """Given a plain source namespace, return the corresponding Namespace
        object, or None if it is not included.
        """
        # Ignore the namespace if it is excluded.
        if plain_src_ns in self._ex_namespace_set:
            return None
        # Include all namespaces if there are no included namespaces.
        if not self._regex_map and not self._plain:
            return Namespace(dest_name=plain_src_ns, source_name=plain_src_ns,
                             include_fields=self._include_fields,
                             exclude_fields=self._exclude_fields)
        # First, search for the namespace in the plain namespaces.
        try:
            return self._plain[plain_src_ns]
        except KeyError:
            # Search for the namespace in the wildcard namespaces.
            for regex, mapped in self._regex_map:
                new_name = match_replace_regex(regex, plain_src_ns,
                                               mapped.dest_name)
                if not new_name:
                    continue
                # Save the new target Namespace in the plain namespaces so
                # future lookups are fast.
                new_mapped = Namespace(
                    dest_name=new_name, source_name=plain_src_ns,
                    include_fields=mapped.include_fields,
                    exclude_fields=mapped.exclude_fields)
                self._add_plain_namespace(new_mapped)
                return new_mapped

        # Save the not included namespace to the excluded namespaces so
        # that future lookups of the same namespace are fast.
        self._ex_namespace_set.add(plain_src_ns)
        return None

    def map_namespace(self, plain_src_ns):
        """Given a plain source namespace, return the corresponding plain
        target namespace, or None if it is not included.
        """
        mapped = self.lookup(plain_src_ns)
        if mapped:
            return mapped.dest_name
        return None

    def unmap_namespace(self, plain_target_ns):
        """Given a plain target namespace, return the corresponding source
        namespace.
        """
        # Return the same namespace if there are no included namespaces.
        if not self._regex_map and not self._plain:
            return plain_target_ns

        src_name_set = self._reverse_plain.get(plain_target_ns)
        if src_name_set:
            # Return the first (and only) item in the set
            for src_name in src_name_set:
                return src_name
        # The target namespace could also exist in the wildcard namespaces
        for _, mapped in self._regex_map:
            original_name = match_replace_regex(
                namespace_to_regex(mapped.dest_name), plain_target_ns,
                mapped.source_name)
            if original_name:
                return original_name
        return None

    def map_db(self, plain_src_db):
        """Given a plain source database, return the list of target databases.

        Individual collections in a database can be mapped to different
        target databases, so map_db can return multiple databases. This
        function must return all target database names so we make the
        following restrictions on wildcards:
        1) A wildcard appearing in the source database name must also appear
        in the target database name, eg "db*.col" => "new_db_*.new_col".
        2) A wildcard appearing in the source collection name must also appear
        in the target collection name, eg "db.col*" => "new_db.new_col*".

        This is used by the CommandHelper for the dropDatabase command.
        """
        if not self._regex_map and not self._plain:
            return [plain_src_db]
        # Lookup this namespace to seed the plain_db dictionary
        self.lookup(plain_src_db + '.$cmd')
        return list(self._plain_db.get(plain_src_db, set()))

    def projection(self, plain_src_name):
        """Return the projection for the given source namespace."""
        mapped = self.lookup(plain_src_name)
        if not mapped:
            return None
        fields = mapped.include_fields or mapped.exclude_fields
        if fields:
            include = 1 if mapped.include_fields else 0
            return dict((field, include) for field in fields)
        return None


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
    for source, target in user_mapping.items():
        if source.count("*") > 1 or target.count("*") > 1:
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' cannot contain more "
                "than one '*' character." % (source, target))
        if source.count("*") != target.count("*"):
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' must contain the "
                "same number of '*' characters." % (source, target))
        if '*' not in source:
            continue
        # Make sure that wildcards are not moved from database name to
        # collection name or vice versa, eg "db*.foo" => "db.foo_*"
        if wildcard_in_db(source) and not wildcard_in_db(target) or (
                    not wildcard_in_db(source) and wildcard_in_db(target)):
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' is invalid. A '*' "
                "that appears in the source database name must also appear"
                "in the target database name. A '*' that appears in the "
                "source collection name must also appear in the target "
                "collection name" % (source, target))

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


def wildcard_in_db(namespace):
    """Return True if a wildcard character appears in the database name."""
    return namespace.find('*') < namespace.find('.')


def namespace_to_regex(namespace):
    """Create a RegexObject from a wildcard namespace."""
    if wildcard_in_db(namespace):
        # A database name cannot contain a '.' character
        wildcard_group = '([^.]*)'
    else:
        wildcard_group = '(.*)'
    return re.compile(r'\A' +
                      re.escape(namespace).replace('\*', wildcard_group) +
                      r'\Z')


def validate_include_fields(include_fields, namespace_fields=None):
    include_fields = set(include_fields or [])
    namespace_fields = set(namespace_fields or [])
    merged = include_fields | namespace_fields
    if merged:
        merged.add('_id')
    return merged


def validate_exclude_fields(exclude_fields, namespace_fields=None):
    exclude_fields = set(exclude_fields or [])
    namespace_fields = set(namespace_fields or [])
    merged = exclude_fields | namespace_fields
    if '_id' in merged:
        LOG.warning("OplogThread: Cannot exclude '_id' field, "
                    "ignoring")
        merged.discard('_id')
    return merged
