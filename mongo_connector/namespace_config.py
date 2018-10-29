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


_Namespace = namedtuple(
    "Namespace",
    ["dest_name", "source_name", "gridfs", "include_fields", "exclude_fields"],
)


class Namespace(_Namespace):
    def __new__(
        cls,
        dest_name=None,
        source_name=None,
        gridfs=False,
        include_fields=None,
        exclude_fields=None,
    ):
        include_fields = set(include_fields or [])
        exclude_fields = set(exclude_fields or [])
        return super(Namespace, cls).__new__(
            cls, dest_name, source_name, gridfs, include_fields, exclude_fields
        )

    def with_options(self, **kwargs):
        new_options = dict(
            dest_name=self.dest_name,
            source_name=self.source_name,
            gridfs=self.gridfs,
            include_fields=self.include_fields,
            exclude_fields=self.exclude_fields,
        )
        new_options.update(kwargs)
        return Namespace(**new_options)


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
            if "*" in ns:
                regexes.add(namespace_to_regex(ns))
            else:
                strings.add(ns)
        return RegexSet(regexes, strings)


class NamespaceConfig(object):
    """Manages included and excluded namespaces.
    """

    def __init__(
        self,
        namespace_set=None,
        ex_namespace_set=None,
        gridfs_set=None,
        dest_mapping=None,
        namespace_options=None,
        include_fields=None,
        exclude_fields=None,
    ):
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

        # Add each included namespace. Namespaces have a one-to-one
        # relationship to the target system, meaning multiple source
        # namespaces cannot be merged into a single namespace in the target.
        ex_namespace_set, namespaces = validate_namespace_options(
            namespace_set=namespace_set,
            ex_namespace_set=ex_namespace_set,
            gridfs_set=gridfs_set,
            dest_mapping=dest_mapping,
            namespace_options=namespace_options,
            include_fields=include_fields,
            exclude_fields=exclude_fields,
        )

        # The set of, possibly wildcard, namespaces to exclude.
        self._ex_namespace_set = RegexSet.from_namespaces(ex_namespace_set)

        for namespace in namespaces:
            self._register_namespace_and_command(namespace)

    def _register_namespace_and_command(self, namespace):
        """Add a Namespace and the corresponding command namespace."""
        self._add_namespace(namespace)
        # Add the namespace for commands on this database
        cmd_name = namespace.source_name.split(".", 1)[0] + ".$cmd"
        dest_cmd_name = namespace.dest_name.split(".", 1)[0] + ".$cmd"
        self._add_namespace(Namespace(dest_name=dest_cmd_name, source_name=cmd_name))

    def _add_namespace(self, namespace):
        """Add an included and possibly renamed Namespace."""
        src_name = namespace.source_name
        if "*" in src_name:
            self._regex_map.append((namespace_to_regex(src_name), namespace))
        else:
            self._add_plain_namespace(namespace)

    def _add_plain_namespace(self, namespace):
        """Add an included and possibly renamed non-wildcard Namespace."""
        src_name = namespace.source_name
        target_name = namespace.dest_name
        src_names = self._reverse_plain.setdefault(target_name, set())
        src_names.add(src_name)
        if len(src_names) > 1:
            # Another source namespace is already mapped to this target
            existing_src = (src_names - set([src_name])).pop()
            raise errors.InvalidConfiguration(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Trying to map '%s' to '%s' but there already "
                "exists a mapping from '%s' to '%s'"
                % (src_name, target_name, existing_src, target_name)
            )

        self._plain[src_name] = namespace
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
            return Namespace(
                dest_name=plain_src_ns,
                source_name=plain_src_ns,
                include_fields=self._include_fields,
                exclude_fields=self._exclude_fields,
            )
        # First, search for the namespace in the plain namespaces.
        try:
            return self._plain[plain_src_ns]
        except KeyError:
            # Search for the namespace in the wildcard namespaces.
            for regex, namespace in self._regex_map:
                new_name = match_replace_regex(regex, plain_src_ns, namespace.dest_name)
                if not new_name:
                    continue
                # Save the new target Namespace in the plain namespaces so
                # future lookups are fast.
                new_namespace = namespace.with_options(
                    dest_name=new_name, source_name=plain_src_ns
                )
                self._add_plain_namespace(new_namespace)
                return new_namespace

        # Save the not included namespace to the excluded namespaces so
        # that future lookups of the same namespace are fast.
        self._ex_namespace_set.add(plain_src_ns)
        return None

    def map_namespace(self, plain_src_ns):
        """Given a plain source namespace, return the corresponding plain
        target namespace, or None if it is not included.
        """
        namespace = self.lookup(plain_src_ns)
        if namespace:
            return namespace.dest_name
        return None

    def gridfs_namespace(self, plain_src_ns):
        """Given a plain source namespace, return the corresponding plain
        target namespace if this namespace is a gridfs collection.
        """
        namespace = self.lookup(plain_src_ns)
        if namespace and namespace.gridfs:
            return namespace.dest_name
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
        for _, namespace in self._regex_map:
            original_name = match_replace_regex(
                namespace_to_regex(namespace.dest_name),
                plain_target_ns,
                namespace.source_name,
            )
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
        self.lookup(plain_src_db + ".$cmd")
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

    def get_included_databases(self):
        """Return the databases we want to include, or empty list for all.
        """
        databases = set()
        databases.update(self._plain_db.keys())

        for _, namespace in self._regex_map:
            database_name, _ = namespace.source_name.split(".", 1)
            if "*" in database_name:
                return []
            databases.add(database_name)

        return list(databases)


def _character_matches(name1, name2):
    """Yield the number of characters that match the beginning of each string.
    """
    if name1[0] == "*":
        for i in range(len(name2) + 1):
            yield 1, i
    if name2[0] == "*":
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


def _validate_namespace(name):
    """Validate a MongoDB namespace."""
    if name.find(".", 1, len(name) - 1) < 0:
        raise errors.InvalidConfiguration("Invalid MongoDB namespace '%s'!" % (name,))


def _validate_namespaces(namespaces):
    """Validate wildcards and renaming in namespaces.

    Target namespaces should have the same number of wildcards as the source.
    No target namespaces overlap exactly with each other. Logs a warning
    when wildcard namespaces have a chance of overlapping.
    """
    for source, namespace in namespaces.items():
        target = namespace.dest_name
        _validate_namespace(source)
        _validate_namespace(target)
        if source.count("*") > 1 or target.count("*") > 1:
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' cannot contain more "
                "than one '*' character." % (source, target)
            )
        if source.count("*") != target.count("*"):
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' must contain the "
                "same number of '*' characters." % (source, target)
            )
        if "*" not in source:
            continue
        # Make sure that wildcards are not moved from database name to
        # collection name or vice versa, eg "db*.foo" => "db.foo_*"
        if (
            wildcard_in_db(source)
            and not wildcard_in_db(target)
            or (not wildcard_in_db(source) and wildcard_in_db(target))
        ):
            raise errors.InvalidConfiguration(
                "The namespace mapping from '%s' to '%s' is invalid. A '*' "
                "that appears in the source database name must also appear"
                "in the target database name. A '*' that appears in the "
                "source collection name must also appear in the target "
                "collection name" % (source, target)
            )

    for source1, source2 in combinations(namespaces.keys(), 2):
        if wildcards_overlap(source1, source2):
            LOG.warning(
                'Namespaces "%s" and "%s" may match the ' "same source namespace.",
                source1,
                source2,
            )
        target1 = namespaces[source1].dest_name
        target2 = namespaces[source2].dest_name
        if target1 == target2:
            raise errors.InvalidConfiguration(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Trying to map '%s' to '%s' but '%s' already "
                "corresponds to '%s' in the target system."
                % (source2, target2, source1, target1)
            )
        if wildcards_overlap(target1, target2):
            LOG.warning(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Mapping from '%s' to '%s' might overlap "
                "with mapping from '%s' to '%s'." % (source2, target2, source1, target1)
            )


def _merge_namespace_options(
    namespace_set=None,
    ex_namespace_set=None,
    gridfs_set=None,
    dest_mapping=None,
    namespace_options=None,
    include_fields=None,
    exclude_fields=None,
):
    """Merges namespaces options together.

    The first is the set of excluded namespaces and the second is a mapping
    from source namespace to Namespace instances.
    """
    namespace_set = set(namespace_set or [])
    ex_namespace_set = set(ex_namespace_set or [])
    gridfs_set = set(gridfs_set or [])
    dest_mapping = dest_mapping or {}
    namespace_options = namespace_options or {}
    include_fields = set(include_fields or [])
    exclude_fields = set(exclude_fields or [])
    namespaces = {}

    for source_name, options_or_str in namespace_options.items():
        if isinstance(options_or_str, dict):
            namespace_set.add(source_name)
            if options_or_str.get("gridfs"):
                gridfs_set.add(source_name)
            namespaces[source_name] = Namespace(
                dest_name=options_or_str.get("rename"),
                include_fields=options_or_str.get("includeFields"),
                exclude_fields=options_or_str.get("excludeFields"),
                gridfs=options_or_str.get("gridfs", False),
            )
        elif isinstance(options_or_str, str):
            namespace_set.add(source_name)
            namespaces[source_name] = Namespace(dest_name=options_or_str)
        elif options_or_str:
            namespace_set.add(source_name)
        else:
            ex_namespace_set.add(source_name)

    # Add namespaces that are renamed but not in namespace_options
    for source_name, target_name in dest_mapping.items():
        namespaces[source_name] = namespaces.get(source_name, Namespace()).with_options(
            dest_name=target_name
        )

    # Add namespaces that are included but not in namespace_options
    for included_name in namespace_set:
        if included_name not in namespaces:
            namespaces[included_name] = Namespace()

    # Add namespaces that are excluded but not in namespace_options
    for gridfs_name in gridfs_set:
        namespaces[gridfs_name] = namespaces.get(gridfs_name, Namespace()).with_options(
            gridfs=True
        )

    # Add source, destination name, and globally included and excluded fields
    for included_name in namespaces:
        namespace = namespaces[included_name]
        namespace = namespace.with_options(
            source_name=included_name,
            include_fields=validate_include_fields(
                include_fields, namespace.include_fields
            ),
            exclude_fields=validate_exclude_fields(
                exclude_fields, namespace.exclude_fields
            ),
        )
        # The default destination name is the same as the source.
        if not namespace.dest_name:
            namespace = namespace.with_options(dest_name=included_name)
        namespaces[included_name] = namespace

    return ex_namespace_set, namespaces


def validate_namespace_options(
    namespace_set=None,
    ex_namespace_set=None,
    gridfs_set=None,
    dest_mapping=None,
    namespace_options=None,
    include_fields=None,
    exclude_fields=None,
):
    ex_namespace_set, namespaces = _merge_namespace_options(
        namespace_set=namespace_set,
        ex_namespace_set=ex_namespace_set,
        gridfs_set=gridfs_set,
        dest_mapping=dest_mapping,
        namespace_options=namespace_options,
        include_fields=include_fields,
        exclude_fields=exclude_fields,
    )

    for excluded_name in ex_namespace_set:
        _validate_namespace(excluded_name)
        if excluded_name in namespaces:
            raise errors.InvalidConfiguration(
                "Cannot include namespace '%s', it is already excluded."
                % (excluded_name,)
            )

    for namespace in namespaces.values():
        if namespace.include_fields and namespace.exclude_fields:
            raise errors.InvalidConfiguration(
                "Cannot mix include fields and exclude fields in "
                "namespace mapping for: '%s'" % (namespace.source_name,)
            )

        if namespace.gridfs and namespace.dest_name != namespace.source_name:
            raise errors.InvalidConfiguration(
                "GridFS namespaces cannot be renamed: '%s'" % (namespace.source_name,)
            )

    _validate_namespaces(namespaces)
    return ex_namespace_set, namespaces.values()


def match_replace_regex(regex, src_namespace, dest_namespace):
    """Return the new mapped namespace if the src_namespace matches the
    regex."""
    match = regex.match(src_namespace)
    if match:
        return dest_namespace.replace("*", match.group(1))
    return None


def wildcard_in_db(namespace):
    """Return True if a wildcard character appears in the database name."""
    return namespace.find("*") < namespace.find(".")


def namespace_to_regex(namespace):
    """Create a RegexObject from a wildcard namespace."""
    db_name, coll_name = namespace.split(".", 1)
    # A database name cannot contain a '.' character
    db_regex = re.escape(db_name).replace(r"\*", "([^.]*)")
    # But a collection name can.
    coll_regex = re.escape(coll_name).replace(r"\*", "(.*)")
    return re.compile(r"\A" + db_regex + r"\." + coll_regex + r"\Z")


def validate_include_fields(include_fields, namespace_fields=None):
    include_fields = set(include_fields or [])
    namespace_fields = set(namespace_fields or [])
    merged = include_fields | namespace_fields
    if merged:
        merged.add("_id")
    return merged


def validate_exclude_fields(exclude_fields, namespace_fields=None):
    exclude_fields = set(exclude_fields or [])
    namespace_fields = set(namespace_fields or [])
    merged = exclude_fields | namespace_fields
    if "_id" in merged:
        LOG.warning("Cannot exclude '_id' field, ignoring")
        merged.discard("_id")
    return merged
