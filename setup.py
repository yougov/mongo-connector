# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

classifiers = """\
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Apache Software License
Programming Language :: Python
Programming Language :: JavaScript
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Unix
"""

try:
    from setuptools import setup
except ImportError:
    from ez_setup import setup
    use_setup_tools()
    from setuptools import setup

setup(name='mongo-connector',
      version="1.1.1+",
      author="MongoDB, Inc.",
      author_email='mongodb-user@googlegroups.com',
      description='Mongo Connector',
      keywords='mongo-connector',
      url='https://github.com/10gen-labs/mongo-connector',
      license="http://www.apache.org/licenses/LICENSE-2.0.html",
      platforms=["any"],
      classifiers=filter(None, classifiers.split("\n")),
      install_requires=['pymongo', 'pyes', 'pysolr >= 3.1.0', 'simplejson'],
      packages=["mongo_connector"],
      test_suite="tests",
      package_data={
          'mongo_connector.doc_managers': ['schema.xml']
      },
      entry_points={
          'console_scripts' : [
              'mongo-connector = mongo_connector.connector:main',
          ],
      }
)
