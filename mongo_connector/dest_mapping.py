import logging
import threading
import re
from mongo_connector import errors


LOG = logging.getLogger(__name__)

"""New structure to handle both plain and
wildcard mapping namespaces dynamically.
"""


class DestMapping():
    def __init__(self, namespace_set, ex_namespace_set, user_mapping):
        # a dict containing plain mappings
        self.plain = {}
        # a dict containing wildcard mappings
        self.wildcard = {}
        # a dict containing reverse plain mapping
        # because the mappings are not duplicated,
        # so the values should also be unique
        self.reverse_plain = {}
        # a dict containing plain db mappings, db -> a set of mapped db
        self.plain_db = {}

        # the input namespace_set and ex_namespace_set could contain wildcard
        self.namespace_set = namespace_set
        self.ex_namespace_set = ex_namespace_set

        self.lock = threading.Lock()

        # initialize
        for ns in namespace_set:
            user_mapping.setdefault(ns, ns)

        for k, v in user_mapping.items():
            self.set(k, v)

    def set_plain(self, key, value):
        """A utility function to set the corresponding plain variables"""
        db, col = key.split(".", 1)
        self.plain[key] = value
        self.reverse_plain[value] = key
        if col != "$cmd":
            if db not in self.plain_db:
                self.plain_db[db] = set([value.split(".")[0]])
            else:
                self.plain_db[db].add(value.split(".")[0])

    def match(self, src, dst_pattern):
        """If source string src matches dst, return the matchobject"""
        reg_pattern = r'\A' + dst_pattern.replace('*', '(.*)') + r'\Z'
        m = re.match(reg_pattern, src)
        return m

    def match_set(self, plain_src, dst_arr):
        for x in dst_arr:
            if plain_src == x:
                return True
            if "*" in x:
                m = self.match(plain_src, x)
                if m:
                    return True

        return False

    def replace(self, src_match, map_pattern):
        """Given the matchobject src_match,
        replace corresponding '*' in map_pattern with src_match."""
        wildcard_matched = src_match.group(1)
        return map_pattern.replace("*", wildcard_matched)

    def get(self, plain_src_ns, defaultval=""):
        """Given a plain source namespace, return a mapped namespace if matched.
        If no match and given defaultval, return it as a default value.
        """
        self.lock.acquire()
        # search in plain mappings first
        if plain_src_ns in self.plain.keys():
            self.lock.release()
            return self.plain[plain_src_ns]
        else:
            # search in wildcard mappings
            # if matched, get a replaced mapped namespace
            # and add to the plain mappings
            for k, v in self.wildcard.items():
                m_res = self.match(plain_src_ns, k)
                if m_res:
                    res = self.replace(m_res, v)
                    if res is not None:
                        if res not in self.reverse_plain:
                            self.set_plain(plain_src_ns, res)
                            self.lock.release()
                            return res
                        else:
                            self.lock.release()
                            raise errors.InvalidConfiguration(
                                "Destination namespaces set wildcard pattern"
                                " conflicts with the plain pattern.")

        if defaultval:
            self.lock.release()
            return defaultval
        else:
            self.lock.release()
            LOG.warn("Failed to find matched mapping for %s." % plain_src_ns)
            return None

    def set(self, key, value):
        """Set the DestMapping instance to corresponding dictionary"""
        self.lock.acquire()

        if "*" in key:
            self.wildcard[key] = value
        else:
            if value not in self.reverse_plain:
                self.set_plain(key, value)
            else:
                raise errors.InvalidConfiguration(
                    "Destination namespaces set should not"
                    " contain any duplicates.")

        self.lock.release()

    def get_key(self, plain_mapped_ns):
        """Given a plain mapped namespace,
         return a source namespace if matched.
        Only need to consider plain mappings
         since this is only called in rollback
         and all the rollback namespaces in the
         target system should already been
         put in the reverse_plain dictionary.
        """
        return self.reverse_plain.get(plain_mapped_ns)

    def map_namespace(self, plain_src_ns):
        """Applies the plain source namespace mapping to a "db.collection" string.
        The input parameter ns is plain text."""
        # if plain_src_ns matches ex_namespace_set, ignore
        if self.match_set(plain_src_ns, self.ex_namespace_set):
            return None
        # if no namespace_set, no renaming
        if not self.namespace_set:
            return plain_src_ns
        else:
            return self.get(plain_src_ns)

    def map_db(self, plain_src_db):
        """Applies the namespace mapping to a database.
        Individual collections in a database can be mapped to
        different target databases, so map_db can return multiple results.
        The input parameter db is plain text.
        This is used to dropDatabase, so we assume before drop, those target
        databases should exist and already been put to plain_db when doing
        create/insert operation.
        """

        if self.plain_db:
            return list(self.plain_db.get(plain_src_db, set([])))
        else:
            return [plain_src_db]
