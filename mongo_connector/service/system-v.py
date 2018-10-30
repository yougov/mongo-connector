"""
Install and Uninstall Mongo Connector as a Linux system daemon
"""

import platform
import os
import shutil
import pathlib

import autocommand
import importlib_resources as res


def check_env():
    if platform.system() != "Linux":
        print("Must be running Linux")
        raise SystemExit(1)
    if os.geteuid() > 0:
        print("Must be root user")
        raise SystemExit(2)


log_path = pathlib.Path("/var/log/mongo-connector")
init_script = pathlib.Path("/etc/init.d/mongo-connector")
config_file = pathlib.Path("/etc/mongo-connector.json")
package = "mongo_connector.service"


def install():
    log_path.mkdir(exist_ok=True)
    init_script.dirname().makedirs_p()
    with res.path(package, "config.json") as config_src_path:
        shutil.copyfile(config_src_path, config_file)
    with res.path(package, "System V init") as init_src_path:
        shutil.copyfile(init_src_path, init_script)


def uninstall():
    shutil.rmtree(log_path)
    config_file.unlink()
    init_script.unlink()


@autocommand.autocommand(__name__)
def run(command):
    check_env()
    globals()[command]()
