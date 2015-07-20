For complete documentation, check out the `Mongo Connector Wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.

DISCLAIMER
----------

Please note: all tools/ scripts in this repo are released for use "AS IS" without any warranties of any kind, including, but not limited to their installation, use, or performance. We disclaim any and all warranties, either express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness for a particular purpose. We do not warrant that the technology will meet your requirements, that the operation thereof will be uninterrupted or error-free, or that any errors will be corrected.
Any use of these scripts and tools is at your own risk. There is no guarantee that they have been through thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with their use.
You are responsible for reviewing and testing any scripts you run thoroughly before use in any non-testing environment.

System Overview
---------------

`mongo-connector` creates a pipeline from a MongoDB cluster to one or more
target systems, such as Solr, Elasticsearch, or another MongoDB cluster.  It
synchronizes data in MongoDB to the target then tails the MongoDB oplog, keeping
up with operations in MongoDB in real-time. It has been tested with Python 2.6,
2.7, 3.3, and 3.4. Detailed documentation is available on the `wiki
<https://github.com/10gen-labs/mongo-connector/wiki>`__.

Getting Started
---------------

Installation
~~~~~~~~~~~~

The easiest way to install mongo-connector is with
`pip <https://pypi.python.org/pypi/pip>`__::

  pip install mongo-connector

You can also install the development version of mongo-connector
manually::

  git clone https://github.com/10gen-labs/mongo-connector.git
  cd mongo-connector
  python setup.py install

You may have to run ``python setup.py install`` with ``sudo``, depending
on where you're installing mongo-connector and what privileges you have.

Using mongo-connector
~~~~~~~~~~~~~~~~~~~~~

mongo-connector replicates operations from the MongoDB oplog, so a
`replica
set <http://docs.mongodb.org/manual/tutorial/deploy-replica-set/>`__
must be running before startup. For development purposes, you may find
it convenient to run a one-node replica set (note that this is **not**
recommended for production)::

  mongod --replSet myDevReplSet

To initialize your server as a replica set, run the following command in
the mongo shell::

  rs.initiate()

Once the replica set is running, you may start mongo-connector. The
simplest invocation resembles the following::

  mongo-connector -m <mongodb server hostname>:<replica set port> \
                  -t <replication endpoint URL, e.g. http://localhost:8983/solr> \
                  -d <name of doc manager, e.g., solr_doc_manager>

mongo-connector has many other options besides those demonstrated above.
To get a full listing with descriptions, try ``mongo-connector --help``.
You can also use mongo-connector with a `configuration file <https://github.com/10gen-labs/mongo-connector/wiki/Configuration-File>`__.

If you want to jump-start into using mongo-connector with a another particular system, check out:

- `Usage with Solr <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20Solr>`__
- `Usage with Elasticsearch <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20ElasticSearch>`__
- `Usage with MongoDB <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20MongoDB>`__

Troubleshooting/Questions
-------------------------

Having trouble with installation? Have a question about Mongo Connector?
Your question or problem may be answered in the `FAQ <https://github.com/10gen-labs/mongo-connector/wiki/FAQ>`__ or in the `wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.
If you can't find the answer to your question or problem there, feel free to `open an issue <https://github.com/10gen-labs/mongo-connector/issues>`__ on Mongo Connector's Github page.
