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
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Unix
Operating System :: MacOS :: MacOS X
Operating System :: Microsoft :: Windows
Operating System :: POSIX
"""

import sys
try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

extra_opts = {"test_suite": "tests"}

if sys.version_info[:2] == (2, 6):
    # Need unittest2 to run unittests in Python 2.6
    extra_opts["tests_require"] = "unittest2"
    extra_opts["test_suite"] = "unittest2.collector"

try:
    with open("README.rst", "r") as fd:
        extra_opts['long_description'] = fd.read()
except IOError:
    pass        # Install without README.rst

setup(name='mongo-connector',
      version="1.2.1",
      author="MongoDB, Inc.",
      author_email='mongodb-user@googlegroups.com',
      description='Mongo Connector',
      keywords=['mongo-connector', 'mongo', 'mongodb', 'solr', 'elasticsearch'],
      url='https://github.com/10gen-labs/mongo-connector',
      license="http://www.apache.org/licenses/LICENSE-2.0.html",
      platforms=["any"],
      classifiers=filter(None, classifiers.split("\n")),
      install_requires=['pymongo >= 2.4', 'pysolr >= 3.1.0', 'elasticsearch'],
      packages=["mongo_connector", "mongo_connector.doc_managers"],
      package_data={
          'mongo_connector.doc_managers': ['schema.xml']
      },
      entry_points={
          'console_scripts': [
              'mongo-connector = mongo_connector.connector:main',
          ],
      },
      **extra_opts
)
