import logging
import threading
import re
from mongo_connector import errors


LOG = logging.getLogger(__name__)

"""New structure to handle both plain and wildcard mapping namespaces dynamically.
"""

class DestMapping():
    def __init__(self, namespace_set=[], ex_namespace_set=[], inpt={}):
        # a dict containing plain mappings
        self.plain = {}
        # a dict containing wildcard mappings
        self.wildcard = {}
        # a dict containing reverse plain mapping
        # because the mappings are not duplicated, so the values should also be unique
        self.reverse_plain = {}
        # a dict containing plain db mappings, db -> an array of mapped db
        self.plain_db = {}
        
        # the input namespace_set and ex_namespace_set could contain wildcard
        self.namespace_set = namespace_set
        self.ex_namespace_set = ex_namespace_set
        
        self.lock = threading.Lock()
        
        # initialize
        for ns in namespace_set:
            if ns not in inpt.keys():
                inpt[ns] = ns

        for k, v in inpt.items():
            self.set(k,v)
            
    def match(self, src, dst_pattern):
        """If source string src matches dst, return the matchobject"""
        d_arr = re.split('(\*)',dst_pattern)
        r_arr = []
        for x in d_arr:
            if x != "*":
                r_arr.append(re.escape(x))
            else:
                r_arr.append("([^*]+)")
                
        reg_pattern = re.compile(''.join(r_arr))
        m = reg_pattern.match(src)
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
        """Given the matchobject src_match, replace corresponding '*' in map_pattern with src_match."""
        num_match = src_match.lastindex
        if num_match != map_pattern.count("*"):
            LOG.error("The number of wildcards in the mapping %s should be %d" %(map_pattern, num_match))
            return None
        pattern_arr = re.split('(\*)', map_pattern)
        map_arr = []
        map_index = 1
        for x in pattern_arr:
            if x != "*":
                map_arr.append(x)
            else:
                map_arr.append(src_match.group(map_index))
                map_index += 1
        
        return ''.join(map_arr)
        
    
    def get(self,plain_src_ns,defaultval=""):
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
                            self.plain[plain_src_ns] = res
                            self.reverse_plain[res] = plain_src_ns
                            
                            # add matched plain db to plain_db dict, but skip $cmd case
                            db, col = plain_src_ns.split(".", 1)
                            if col != "$cmd":
                                arr = self.plain_db.get(db, [])
                                arr.append(res.split(".")[0])
                                self.plain_db[db] = list(set(arr))
                            self.lock.release()
                            return res
                        else:
                            self.lock.release()
                            raise errors.InvalidConfiguration("Destination namespaces set wildcard pattern conflicts with the plain pattern.")
            
        if defaultval:
            self.lock.release()
            return defaultval
        else:
            self.lock.release()
            LOG.error("Failed to find matched mapping.")
            return None
    

    def set(self, key, value):
        """Set the DestMapping instance to corresponding dictionary"""
        self.lock.acquire()
        db = key.split(".")[0]
        if "*" in db:
            raise errors.InvalidConfiguration("Wildcard is not allowed in db name")
            
        if "*" in key:
            if key.count("*") != value.count("*"):
                raise errors.InvalidConfiguration("The number of wildcards in the mapping should be equal")
            self.wildcard[key] = value
        else:
            if value not in self.reverse_plain:
                self.plain[key] = value
                self.reverse_plain[value] = key
                arr = self.plain_db.get(db, [])
                arr.append(value.split(".")[0])
                self.plain_db[db] = list(set(arr))
            else:
                raise errors.InvalidConfiguration(
                "Destination namespaces set should not"
                " contain any duplicates.")
            
        self.lock.release()

    def get_key(self, plain_mapped_ns):
        """Given a plain mapped namespace, return a source namespace if matched.
        Only need to consider plain mappings since this is only called in rollback
         and all the rollback namespaces in the target system should already been 
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
        # if plain_src_ns does not match namespace_set, ignore
        elif not self.match_set(plain_src_ns, self.namespace_set):
            return None
        else:
            return self.get(plain_src_ns, plain_src_ns)
        
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
            return self.plain_db.get(plain_src_db, [])
        else:
            return [plain_src_db]