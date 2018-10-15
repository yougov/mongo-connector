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

from mongo_connector.namespace_config import NamespaceConfig

LOG = logging.getLogger(__name__)


class CommandHelper(object):
    def __init__(self, namespace_config=None):
        if namespace_config is None:
            namespace_config = NamespaceConfig()
        self.namespace_config = namespace_config

    # Applies the namespace mapping to a database.
    # Individual collections in a database can be mapped to
    # different target databases, so map_db can return multiple results.
    # The input parameter db is plain text
    def map_db(self, db):
        return self.namespace_config.map_db(db)

    # Applies the namespace mapping to a "db.collection" string
    # The input parameter ns is plain text
    def map_namespace(self, ns):
        return self.namespace_config.map_namespace(ns)

    # Applies the namespace mapping to a db and collection
    # The input parameter db and coll are plain text
    def map_collection(self, db, coll):
        ns = self.map_namespace(db + "." + coll)
        if ns:
            return tuple(ns.split(".", 1))
        else:
            return None, None
