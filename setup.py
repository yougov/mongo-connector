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

classifiers = """\
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Apache Software License
Programming Language :: Python :: 2.6
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Unix
Operating System :: MacOS :: MacOS X
Operating System :: Microsoft :: Windows
Operating System :: POSIX
"""

import os
import platform
import sys
from distutils.core import Command
from distutils.dir_util import mkpath, remove_tree
from distutils.file_util import copy_file
try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

extra_opts = {"test_suite": "tests",
              "tests_require": ["mongo-orchestration>=0.6.7,<1.0",
                                "requests>=2.5.1"]}

if sys.version_info[:2] == (2, 6):
    # Need unittest2 to run unittests in Python 2.6
    extra_opts["tests_require"].append("unittest2")
    extra_opts["test_suite"] = "unittest2.collector"

try:
    with open("README.rst", "r") as fd:
        extra_opts['long_description'] = fd.read()
except IOError:
    pass        # Install without README.rst


class InstallService(Command):
    description = "Installs Mongo Connector as a Linux system daemon"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        if platform.system() != 'Linux':
            print("Must be running Linux")
        elif os.geteuid() > 0:
            print("Must be root user")
        else:
            mkpath("/var/log/mongo-connector")
            mkpath("/etc/init.d")
            copy_file("./config.json", "/etc/mongo-connector.json")
            copy_file("./scripts/mongo-connector",
                      "/etc/init.d/mongo-connector")


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
        if platform.system() != 'Linux':
            print("Must be running Linux")
        elif os.geteuid() > 0:
            print("Must be root user")
        else:
            if os.path.exists("/var/log/mongo-connector"):
                remove_tree("/var/log/mongo-connector")
            self.remove_file("/etc/mongo-connector.json")
            self.remove_file("/etc/init.d/mongo-connector")

extra_opts['cmdclass'] = {
    "install_service": InstallService,
    "uninstall_service": UninstallService
}

setup(name='mongo-connector',
      version="2.5.0",
      author="MongoDB, Inc.",
      author_email='mongodb-user@googlegroups.com',
      description='Mongo Connector',
      keywords=['mongo-connector', 'mongo', 'mongodb', 'solr', 'elasticsearch'],
      url='https://github.com/10gen-labs/mongo-connector',
      license="http://www.apache.org/licenses/LICENSE-2.0.html",
      platforms=["any"],
      classifiers=filter(None, classifiers.split("\n")),
      install_requires=['pymongo >= 2.9'],
      packages=["mongo_connector", "mongo_connector.doc_managers"],
      package_data={
          'mongo_connector.doc_managers': ['schema.xml']
      },
      entry_points={
          'console_scripts': [
              'mongo-connector = mongo_connector.connector:main',
          ],
      },
      extras_require={
          "solr": ["solr-doc-manager"],
          "elastic": ["elastic-doc-manager"],
          "elastic-aws": ["elastic-doc-manager[aws]"],
          "elastic2": ["elastic2-doc-manager[elastic2]"],
          "elastic5": ["elastic2-doc-manager[elastic5]"],
          "elastic2-aws": ["elastic2-doc-manager[elastic2,aws]"],
      },
      **extra_opts
)
