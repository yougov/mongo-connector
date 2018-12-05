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

setup(
    name="mongo-connector",
    use_scm_version=True,
    author="MongoDB, Inc.",
    author_email="mongodb-user@googlegroups.com",
    description="Mongo Connector",
    keywords=["mongo-connector", "mongo", "mongodb", "solr", "elasticsearch"],
    url="https://github.com/yougov/mongo-connector",
    platforms=["any"],
    classifiers=filter(None, classifiers.split("\n")),
    install_requires=[
        "pymongo >= 2.9",
        "importlib_metadata>=0.6",
        "autocommand",
        "importlib_resources",
    ],
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
    setup_requires=[
        "setuptools_scm>=1.5",
    ],
    python_requires=">=3.4",
)
