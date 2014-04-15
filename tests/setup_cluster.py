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

"""
Utilities for spawning and killing mongodb clusters for use with the tests
"""

from itertools import count
import os
import shutil
import socket
import subprocess
import time

from pymongo import MongoClient
from tests import mongo_host, mongo_start_port
from tests.util import assert_soon

free_port = count(mongo_start_port)
nodes = {}

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(HERE, "data")
LOG_ROOT = os.path.join(HERE, "logs")
KEY_FILE = os.environ.get("KEY_FILE")


def create_dir(dir_path):
    """Create supplied directory
    """
    try:
        os.makedirs(dir_path)
    except OSError:     # directory may exist already
        pass


def wait_for_proc(proc, port):
    '''Wait for a mongo process to start on a given port'''
    attempts = 0
    while proc.poll() is None and attempts < 160:
        attempts += 1
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect((mongo_host, port))
                return True
            except (IOError, socket.error):
                time.sleep(0.25)
        finally:
            s.close()
    kill_all()
    return False


def start_mongo_proc(proc="mongod", options=[]):
    '''Start a single mongod or mongos process.
    Returns the port on which the process was started.

    '''
    # Build command
    next_free_port = next(free_port)
    dbpath = os.path.join(DATA_ROOT, str(next_free_port))
    logpath = os.path.join(LOG_ROOT, "%d.log" % next_free_port)
    cmd = [proc, "--port", next_free_port,
           "--logpath", logpath, "--logappend",
           "--setParameter", "enableTestCommands=1"] + options

    # Refresh directories
    create_dir(os.path.dirname(logpath))
    if proc == 'mongod':
        cmd.extend(['--dbpath', dbpath])
        shutil.rmtree(dbpath, ignore_errors=True)
        create_dir(dbpath)

    # Start the process
    cmd = list(map(str, cmd))
    mongo = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
    nodes[next_free_port] = {"proc": mongo, "cmd": cmd}
    # Wait until a connection can be established
    wait_for_proc(mongo, next_free_port)
    return next_free_port


def restart_mongo_proc(port):
    '''Restart a mongo process by port number that was killed'''
    mongo = subprocess.Popen(nodes[port]['cmd'],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
    nodes[port]['proc'] = mongo
    wait_for_proc(mongo, port)
    return port


def kill_mongo_proc(port, destroy=True):
    '''Kill a mongod or mongos process by port number'''
    nodes[port]['proc'].terminate()
    if destroy:
        shutil.rmtree(os.path.join(DATA_ROOT, str(port)), ignore_errors=True)
        os.unlink(os.path.join(LOG_ROOT, "%d.log" % port))
        nodes.pop(port)


def start_replica_set(set_name, num_members=3, num_arbiters=1):
    '''Start a replica set with a given name'''
    # Start members
    repl_ports = [start_mongo_proc(options=["--replSet", set_name,
                                            "--noprealloc",
                                            "--nojournal"])
                  for i in range(num_members)]
    # Initialize set
    client = MongoClient(mongo_host, repl_ports[-1])
    client.admin.command(
        'replSetInitiate',
        {
            '_id': set_name, 'members': [
                {
                    '_id': i,
                    'host': '%s:%d' % (mongo_host, p),
                    'arbiterOnly': (i < num_arbiters)
                }
                for i, p in enumerate(repl_ports)
            ]
        }
    )
    # Wait for primary
    assert_soon(lambda: client.admin.command("isMaster")['ismaster'])
    # Wait for secondaries
    for port in repl_ports[num_arbiters:-1]:
        secondary_client = MongoClient(mongo_host, port)
        assert_soon(
            lambda: secondary_client.admin.command(
                'replSetGetStatus')['myState'] == 2)
    # Tag primary
    nodes[client.port]['master'] = True
    # Tag arbiters
    for port in repl_ports[:num_arbiters]:
        nodes[port]['arbiter'] = True
    return repl_ports


def kill_replica_set(set_name):
    '''Kill a replica set by name'''
    for port, proc_doc in list(nodes.items()):
        if set_name in proc_doc['cmd']:
            kill_mongo_proc(port)


def start_cluster(num_shards=2):
    '''Start a sharded cluster. Returns the port of the mongos'''
    # Config
    config_port = start_mongo_proc(options=["--configsvr",
                                            "--noprealloc",
                                            "--nojournal"])

    # Mongos
    mongos_port = start_mongo_proc(proc="mongos", options=[
        "--configdb", "%s:%d" % (mongo_host, config_port)
    ])
    nodes[mongos_port]['config'] = config_port

    # Shards
    client = MongoClient(mongo_host, mongos_port)
    for shard in range(num_shards):
        shard_name = "demo-set-%d" % shard
        repl_ports = start_replica_set(shard_name)
        nodes[mongos_port].setdefault("shards", []).append(shard_name)
        shard_str = "%s/%s" % (shard_name, ",".join("%s:%d" % (mongo_host, port)
                                                    for port in repl_ports))
        client.admin.command("addShard", shard_str)
    return mongos_port


def get_shard(mongos_port, index):
    '''Return a document containing the primary and secondaries of a replica
    set shard by mongos port and shard index:

    {'primary': 12345, 'secondaries': [12121], 'arbiters': [23232]}

    '''
    repl = nodes[mongos_port]['shards'][index]
    doc = {}
    for port, node in nodes.items():
        if repl in node['cmd']:
            if node.get('master'):
                doc['primary'] = port
            elif node.get('arbiter'):
                doc.setdefault('arbiters', []).append(port)
            else:
                doc.setdefault('secondaries', []).append(port)
    return doc


def kill_cluster(mongos_port):
    '''Kill mongod and mongos processes in a sharded cluster by mongos port'''
    for shard in nodes[mongos_port].get("shards", []):
        kill_replica_set(shard)
    kill_mongo_proc(nodes[mongos_port]['config'])
    kill_mongo_proc(mongos_port)


def kill_all():
    """Kill all mongod and mongos instances"""
    for port in list(nodes.keys()):
        kill_mongo_proc(port)
    shutil.rmtree(LOG_ROOT, ignore_errors=True)
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
