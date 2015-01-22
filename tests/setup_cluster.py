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
import os

import requests

_mo_address = os.environ.get("MO_ADDRESS", "localhost:8889")
_mongo_start_port = int(os.environ.get("MONGO_PORT", 27017))
_free_port = itertools.count(start=_mongo_start_port)

DEFAULT_OPTIONS = {
    'logappend': True,
    'setParameter': {'enableTestCommands': 1}
}


def _proc_params(mongos=False):
    params = dict(port=next(_free_port), **DEFAULT_OPTIONS)
    if not mongos:
        params['smallfiles'] = True
        params['noprealloc'] = True
        params['nojournal'] = True

    return params


def _replica_set_config():
    return {
        'members': [
            {'procParams': _proc_params()},
            {'procParams': _proc_params()},
            {'rsParams': {'arbiterOnly': True},
             'procParams': _proc_params()}
        ]
    }


def _sharded_cluster_config():
    return {
        'shards': [
            {'id': 'demo-set-0', 'shardParams': _replica_set_config()},
            {'id': 'demo-set-1', 'shardParams': _replica_set_config()}
        ],
        'routers': [_proc_params(mongos=True)],
        'configsvrs': [_proc_params()]
    }


def _mo_url(resource, *args):
    return 'http://' + '/'.join([_mo_address, resource] + list(args))


@atexit.register
def kill_all():
    clusters = requests.get(_mo_url('sharded_clusters')).json()
    repl_sets = requests.get(_mo_url('replica_sets')).json()
    servers = requests.get(_mo_url('servers')).json()
    for cluster in clusters['sharded_clusters']:
        requests.delete(_mo_url('sharded_clusters', cluster['id']))
    for rs in repl_sets['replica_sets']:
        requests.delete(_mo_url('relica_sets', rs['id']))
    for server in servers['servers']:
        requests.delete(_mo_url('servers', server['id']))


class Server(object):

    def __init__(self, id=None, uri=None):
        self.id = id
        self.uri = uri

    def start(self):
        if self.id is None:
            response = requests.post(_mo_url('servers'), timeout=None, json={
                'name': 'mongod',
                'procParams': _proc_params()
            }).json()
            self.id = response['id']
            self.uri = response['mongodb_uri']
        else:
            requests.post(
                _mo_url('servers', self.id), timeout=None,
                json={'action': 'start'}
            )
        return self

    def stop(self, destroy=True):
        if destroy:
            requests.delete(_mo_url('servers', self.id))
        else:
            requests.post(_mo_url('servers', self.id), timeout=None,
                          json={'action': 'stop'})


class ReplicaSet(object):

    def __init__(self, id=None, uri=None, primary=None, secondary=None):
        self.id = id
        self.uri = uri
        self.primary = primary
        self.secondary = secondary

    def _init_from_response(self, response):
        self.id = response['id']
        self.uri = response['mongodb_uri']
        for member in response['members']:
            if member['state'] == 1:
                self.primary = Server(member['server_id'], member['host'])
            elif member['state'] == 2:
                self.secondary = Server(member['server_id'], member['host'])
        return self

    def start(self):
        # We never need to restart a replica set, only start new ones.
        response = requests.post(
            _mo_url('replica_sets'), timeout=None, json=_replica_set_config())
        self._init_from_response(response.json())
        return self

    def stop(self):
        # We never need only to stop a replica set. We want to blow it up.
        requests.delete(_mo_url('replica_sets', self.id))


class ShardedCluster(object):

    def __init__(self):
        self.id = None
        self.uri = None
        self.shards = []

    def start(self):
        # We never need to restart a sharded cluster, only start new ones.
        response = requests.post(
            _mo_url('sharded_clusters'), timeout=None,
            json=_sharded_cluster_config())
        content = response.json()
        for shard in content['shards']:
            if shard['id'] == 'demo-set-0':
                repl1_id = shard['_id']
            elif shard['id'] == 'demo-set-1':
                repl2_id = shard['_id']
        shard1 = requests.get(_mo_url('replica_sets', repl1_id)).json()
        shard2 = requests.get(_mo_url('replica_sets', repl2_id)).json()
        self.id = content['id']
        self.uri = content['mongodb_uri']
        self.shards = [ReplicaSet()._init_from_response(resp)
                       for resp in (shard1, shard2)]
        return self

    def stop(self):
        # We never need only to stop a sharded cluster. We want to blow it up.
        requests.delete(_mo_url('sharded_clusters', self.id))
