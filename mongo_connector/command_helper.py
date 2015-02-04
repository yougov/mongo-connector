# Copyright 2013-2014 MongoDB, Inc.
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

"""Preprocesses the oplog command entries.
"""

import logging
import mongo_connector.errors

LOG = logging.getLogger(__name__)


class CommandHelper(object):
    def __init__(self, namespace_set=[], dest_mapping={}):
        self.namespace_set = namespace_set
        self.dest_mapping = dest_mapping

        # Create a db to db mapping from the namespace mapping.
        db_pairs = set((ns.split('.')[0],
                        self.map_namespace(ns).split('.')[0])
                       for ns in self.namespace_set)
        targets = set()
        for _, dest in db_pairs:
            if dest in targets:
                dbs = [src2 for src2, dest2 in db_pairs
                       if dest == dest2]
                raise mongo_connector.errors.MongoConnectorError(
                    "Database mapping is not one-to-one."
                    " %s %s have collections mapped to %s"
                    % (", ".join(dbs),
                       "both" if len(dbs) == 2 else "all",
                       dest))
            else:
                targets.add(dest)

        self.db_mapping = {}
        for src, dest in db_pairs:
            arr = self.db_mapping.get(src, [])
            arr.append(dest)
            self.db_mapping[src] = arr

    # Applies the namespace mapping to a database.
    # Individual collections in a database can be mapped to
    # different target databases, so map_db can return multiple results.
    def map_db(self, db):
        if self.db_mapping:
            return self.db_mapping.get(db, [])
        else:
            return [db]

    # Applies the namespace mapping to a "db.collection" string
    def map_namespace(self, ns):
        if not self.namespace_set:
            return ns
        elif ns not in self.namespace_set:
            return None
        else:
            return self.dest_mapping.get(ns, ns)

    # Applies the namespace mapping to a db and collection
    def map_collection(self, db, coll):
        ns = self.map_namespace(db + '.' + coll)
        if ns:
            return tuple(ns.split('.', 1))
        else:
            return None, None
