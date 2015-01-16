import itertools

import requests

from collections import namedtuple

from tests import mongo_start_port, mo_address

_free_port = itertools.count(start=mongo_start_port)

Standalone = namedtuple('Standalone', ('id', 'uri'))
ReplicaSet = namedtuple('ReplicaSet', ('id', 'uri', 'primary', 'secondary'))
ShardedCluster = namedtuple('ShardedCluster', ('id', 'uri', 'shards'))

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
    return 'http://' + '/'.join([mo_address, resource] + list(args))


def start_server(server=None):
    if server is None:
        response = requests.post(_mo_url('servers'), timeout=None, json={
            'name': 'mongod',
            'procParams': _proc_params()
        }).json()
        server = Standalone(response['id'], response['mongodb_uri'])
    else:
        response = requests.post(
            _mo_url('servers', server.id), timeout=None,
            json={'action': 'start'}
        )
    return server


def stop_server(server, destroy=True):
    if destroy:
        response = requests.delete(_mo_url('servers', server.id))
    else:
        response = requests.post(
            _mo_url('servers', server.id), timeout=None,
            json={'action': 'stop'})


def _get_replica_set(repl_info):
    for member in repl_info['members']:
        if member['state'] == 1:
            primary = Standalone(member['server_id'], member['host'])
        elif member['state'] == 2:
            secondary = Standalone(member['server_id'], member['host'])
    return ReplicaSet(
        repl_info['id'], repl_info['mongodb_uri'], primary, secondary)


def start_replica_set():
    # We never need to restart a replica set, only start new ones.
    response = requests.post(
        _mo_url('replica_sets'), timeout=None, json=_replica_set_config())
    content = response.json()
    return _get_replica_set(content)


def stop_replica_set(repl):
    # We never need only to stop a replica set. We want to blow it up.
    response = requests.delete(_mo_url('replica_sets', repl.id))


def start_sharded_cluster():
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
    shards = [_get_replica_set(rs) for rs in (shard1, shard2)]
    return ShardedCluster(content['id'], content['mongodb_uri'],
                          shards)


def stop_sharded_cluster(cluster):
    # We never need only to stop a sharded cluster. We want to blow it up.
    response = requests.delete(_mo_url('sharded_clusters', cluster.id))


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


kill_all()
