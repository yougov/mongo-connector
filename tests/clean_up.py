import subprocess
import sys
import os
import time
import inspect
from os import path

try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection    

""" Global path variables
    """
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
             "CONFIG": "27220", "MONGOS": "27217"}
CURRENT_DIR = inspect.getfile(inspect.currentframe())
CMD_DIR = os.path.realpath(os.path.abspath(os.path.split(CURRENT_DIR)[0]))
SETUP_DIR = path.expanduser(CMD_DIR)
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]


def kill_mongo_proc(host, port):
    """ Kill given port
        """
    try:
        conn = Connection(host, int(port))
        conn['admin'].command('shutdown', 1, force=True)
    except:
        cmd = ["pgrep -f \"" + str(port) + MONGOD_KSTR + "\" | xargs kill -9"]
        execute_command(cmd)


def kill_mongos_proc():
    """ Kill all mongos proc
        """
    cmd = ["pgrep -f \"" + MONGOS_KSTR + "\" | xargs kill -9"]
    execute_command(cmd)


def kill_all_mongo_proc(host, ports):
    """Kill any existing mongods
        """
    for port in ports.values():
        kill_mongo_proc(host, port)


def remove_dir(path):
    """Remove supplied directory
        """
    command = ["rm", "-rf", path]
    subprocess.Popen(command).communicate()


def execute_command(command):
    """Wait a little and then execute shell command
        """
    time.sleep(1)
    #return os.system(command)
    subprocess.Popen(command, shell=True)


if __name__ == "__main__":
    remove_dir(DEMO_SERVER_LOG)
    remove_dir(DEMO_SERVER_DATA)
    # Kill all spawned mongods
    kill_all_mongo_proc('localhost', PORTS_ONE)
    kill_all_mongo_proc('localhost', PORTS_TWO)

    # Kill all spawned mongos
    kill_mongos_proc()
