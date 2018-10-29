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


import os
import platform
import sys
from distutils.core import Command
from distutils.dir_util import mkpath, remove_tree
from distutils.file_util import copy_file
from setuptools import setup

classifiers = """\
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Apache Software License
Programming Language :: Python :: 3
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Unix
Operating System :: MacOS :: MacOS X
Operating System :: Microsoft :: Windows
Operating System :: POSIX
"""

extra_opts = {}

try:
    with open("README.rst", "r") as fd:
        extra_opts["long_description"] = fd.read()
except IOError:
    pass  # Install without README.rst


class InstallService(Command):
    description = "Installs Mongo Connector as a Linux system daemon"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        if platform.system() != "Linux":
            print("Must be running Linux")
        elif os.geteuid() > 0:
            print("Must be root user")
        else:
            mkpath("/var/log/mongo-connector")
            mkpath("/etc/init.d")
            copy_file("./config.json", "/etc/mongo-connector.json")
            copy_file("./scripts/mongo-connector", "/etc/init.d/mongo-connector")


class UninstallService(Command):
    description = "Uninstalls Mongo Connector as a Linux system daemon"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def remove_file(self, path):
        if os.path.exists(path):
            os.remove(path)
            print("removing '%s'" % path)

    def run(self):
        if platform.system() != "Linux":
            print("Must be running Linux")
        elif os.geteuid() > 0:
            print("Must be root user")
        else:
            if os.path.exists("/var/log/mongo-connector"):
                remove_tree("/var/log/mongo-connector")
            self.remove_file("/etc/mongo-connector.json")
            self.remove_file("/etc/init.d/mongo-connector")


extra_opts["cmdclass"] = {
    "install_service": InstallService,
    "uninstall_service": UninstallService,
}

setup(
    name="mongo-connector",
    version="3.0.0",
    author="MongoDB, Inc.",
    author_email="mongodb-user@googlegroups.com",
    description="Mongo Connector",
    keywords=["mongo-connector", "mongo", "mongodb", "solr", "elasticsearch"],
    url="https://github.com/yougov/mongo-connector",
    license="http://www.apache.org/licenses/LICENSE-2.0.html",
    platforms=["any"],
    classifiers=filter(None, classifiers.split("\n")),
    install_requires=["pymongo >= 2.9", "importlib_metadata>=0.6"],
    packages=["mongo_connector", "mongo_connector.doc_managers"],
    package_data={"mongo_connector.doc_managers": ["schema.xml"]},
    entry_points={
        "console_scripts": ["mongo-connector = mongo_connector.connector:main"]
    },
    extras_require={
        "solr": ["solr-doc-manager"],
        "elastic": ["elastic-doc-manager"],
        "elastic-aws": ["elastic-doc-manager[aws]"],
        "elastic2": ["elastic2-doc-manager[elastic2]"],
        "elastic5": ["elastic2-doc-manager[elastic5]"],
        "elastic2-aws": ["elastic2-doc-manager[elastic2,aws]"],
    },
    python_requires=">=3.4",
    **extra_opts
)
