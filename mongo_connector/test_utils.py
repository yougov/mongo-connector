# Copyright 2015 MongoDB, Inc.
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

import atexit
import itertools
import time
import os
import sys

import pymongo
import requests


if sys.version_info[0] == 3:
    unicode = str

# Configurable hosts and ports used in the tests
db_user = unicode(os.environ.get("DB_USER", ""))
db_password = unicode(os.environ.get("DB_PASSWORD", ""))
# Extra keyword options to provide to Connector.
connector_opts = {}
if db_user:
    connector_opts = {"auth_username": db_user, "auth_key": db_password}

# Document count for stress tests
STRESS_COUNT = 100

# Test namespace, timestamp arguments
TESTARGS = ("test.test", 1)

_mo_address = os.environ.get("MO_ADDRESS", "localhost:8889")
_mongo_start_port = int(os.environ.get("MONGO_PORT", 27017))
_free_port = itertools.count(_mongo_start_port)

DEFAULT_OPTIONS = {"logappend": True, "setParameter": {"enableTestCommands": 1}}


_post_request_template = {}
if db_user and db_password:
    _post_request_template = {"login": db_user, "password": db_password}


def _mo_url(resource, *args):
    return "http://" + "/".join([_mo_address, resource] + list(args))


@atexit.register
def kill_all():
    clusters = requests.get(_mo_url("sharded_clusters")).json()
    repl_sets = requests.get(_mo_url("replica_sets")).json()
    servers = requests.get(_mo_url("servers")).json()
    for cluster in clusters["sharded_clusters"]:
        requests.delete(_mo_url("sharded_clusters", cluster["id"]))
    for rs in repl_sets["replica_sets"]:
        requests.delete(_mo_url("relica_sets", rs["id"]))
    for server in servers["servers"]:
        requests.delete(_mo_url("servers", server["id"]))


class MCTestObject(object):
    def proc_params(self):
        params = DEFAULT_OPTIONS.copy()
        params.update(self._proc_params)
        params["port"] = next(_free_port)
        return params

    def get_config(self):
        raise NotImplementedError

    def _make_post_request(self):
        config = _post_request_template.copy()
        config.update(self.get_config())
        ret = requests.post(_mo_url(self._resource), timeout=None, json=config)
        if not ret.ok:
            raise RuntimeError("Error sending POST to cluster: %s" % (ret.text,))

        ret = ret.json()
        if type(ret) == list:  # Will return a list if an error occurred.
            raise RuntimeError("Error sending POST to cluster: %s" % (ret,))
        return ret

    def client(self, **kwargs):
        client = pymongo.MongoClient(self.uri, **kwargs)
        if db_user:
            client.admin.authenticate(db_user, db_password)
        return client

    def stop(self):
        requests.delete(_mo_url(self._resource, self.id))


class Server(MCTestObject):

    _resource = "servers"

    def __init__(self, id=None, uri=None, **kwargs):
        self.id = id
        self.uri = uri
        self._proc_params = kwargs

    def get_config(self):
        return {"name": "mongod", "procParams": self.proc_params()}

    def start(self):
        if self.id is None:
            response = self._make_post_request()
            self.id = response["id"]
            self.uri = response.get("mongodb_auth_uri", response["mongodb_uri"])
        else:
            requests.post(
                _mo_url("servers", self.id), timeout=None, json={"action": "start"}
            )
        return self

    def stop(self, destroy=True):
        if destroy:
            super(Server, self).stop()
        else:
            requests.post(
                _mo_url("servers", self.id), timeout=None, json={"action": "stop"}
            )


class ReplicaSet(MCTestObject):

    _resource = "replica_sets"

    def __init__(self, id=None, uri=None, primary=None, secondary=None, **kwargs):
        self.id = id
        self.uri = uri
        self.primary = primary
        self.secondary = secondary
        self._proc_params = kwargs

    def get_config(self):
        return {
            "members": [
                {"procParams": self.proc_params()},
                {"procParams": self.proc_params()},
                {"rsParams": {"arbiterOnly": True}, "procParams": self.proc_params()},
            ]
        }

    def _init_from_response(self, response):
        self.id = response["id"]
        self.uri = response.get("mongodb_auth_uri", response["mongodb_uri"])
        for member in response["members"]:
            if member["state"] == 1:
                self.primary = Server(member["server_id"], member["host"])
            elif member["state"] == 2:
                self.secondary = Server(member["server_id"], member["host"])
        return self

    def start(self):
        # We never need to restart a replica set, only start new ones.
        return self._init_from_response(self._make_post_request())


class ReplicaSetSingle(ReplicaSet):
    def get_config(self):
        return {"members": [{"procParams": self.proc_params()}]}


class ShardedCluster(MCTestObject):

    _resource = "sharded_clusters"
    _shard_type = ReplicaSet

    def __init__(self, **kwargs):
        self.id = None
        self.uri = None
        self.shards = []
        self._proc_params = kwargs

    def get_config(self):
        return {
            "shards": [
                {"id": "demo-set-0", "shardParams": self._shard_type().get_config()},
                {"id": "demo-set-1", "shardParams": self._shard_type().get_config()},
            ],
            "routers": [self.proc_params()],
            "configsvrs": [self.proc_params()],
        }

    def start(self):
        # We never need to restart a sharded cluster, only start new ones.
        response = self._make_post_request()
        for shard in response["shards"]:
            if shard["id"] == "demo-set-0":
                repl1_id = shard["_id"]
            elif shard["id"] == "demo-set-1":
                repl2_id = shard["_id"]
        shard1 = requests.get(_mo_url("replica_sets", repl1_id)).json()
        shard2 = requests.get(_mo_url("replica_sets", repl2_id)).json()
        self.id = response["id"]
        self.uri = response.get("mongodb_auth_uri", response["mongodb_uri"])
        self.shards = [
            self._shard_type()._init_from_response(resp) for resp in (shard1, shard2)
        ]
        return self


class ShardedClusterSingle(ShardedCluster):
    _shard_type = ReplicaSetSingle


class MockGridFSFile:
    def __init__(self, doc, data):
        self._id = doc["_id"]
        self.filename = doc["filename"]
        self.upload_date = doc["upload_date"]
        self.md5 = doc["md5"]
        self.data = data
        self.length = len(self.data)
        self.pos = 0

    def get_metadata(self):
        return {
            "_id": self._id,
            "filename": self.filename,
            "upload_date": self.upload_date,
            "md5": self.md5,
        }

    def __len__(self):
        return self.length

    def read(self, n=-1):
        if n < 0 or self.pos + n > self.length:
            n = self.length - self.pos
        s = self.data[self.pos : self.pos + n]
        self.pos += n
        return s


def wait_for(condition, max_tries=60):
    """Wait for a condition to be true up to a maximum number of tries
    """
    cond = False
    while not cond and max_tries > 1:
        try:
            cond = condition()
        except Exception:
            pass
        time.sleep(1)
        max_tries -= 1
    return condition()


def assert_soon(condition, message=None, max_tries=60):
    """Assert that a condition eventually evaluates to True after at most
    max_tries number of attempts

    """
    if not wait_for(condition, max_tries=max_tries):
        raise AssertionError(message or "")


def close_client(client):
    if hasattr(type(client), "_process_kill_cursors_queue"):
        client._process_kill_cursors_queue()
        time.sleep(1)  # Wait for queue to clear.
    client.close()
