===============
mongo-connector
===============

.. image:: https://travis-ci.org/mongodb-labs/mongo-connector.svg?branch=master
   :alt: View build status
   :target: https://travis-ci.org/mongodb-labs/mongo-connector

For complete documentation, check out the `Mongo Connector Wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.

System Overview
---------------

`mongo-connector` creates a pipeline from a MongoDB cluster to one or more
target systems, such as Solr, Elasticsearch, or another MongoDB cluster.  It
synchronizes data in MongoDB to the target then tails the MongoDB oplog, keeping
up with operations in MongoDB in real-time. It has been tested with Python 2.6,
2.7, and 3.3+. Detailed documentation is available on the `wiki
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
- Usage with Splunk
	1. Open mongo_connector/doc_managers/splunk_doc_manager.py then change with Splunk's username and password at line 57 and 58.
	2. Install Splunk Python SDK and set environment variable to point the SDK
		e.g., export PYTHONPATH=~/splunk-sdk-python
	3. Run the mongo-connector.
		e.g., mongo-connector -m mongo-ip:27017 -t http://splunk-ip:8089 -d splunk_doc_manager.py

		I have used default ports in above example for MongoDB and Splunk, you can change as required.

Doc Managers
~~~~~~~~~~~~~~~~~~~~~

Elastic 1.x doc manager: https://github.com/mongodb-labs/elastic-doc-manager

Elastic 2.x doc manager: https://github.com/mongodb-labs/elastic2-doc-manager

The Solr doc manager and the MongoDB doc manager come packaged with the mongo-connector project.

Troubleshooting/Questions
-------------------------

Having trouble with installation? Have a question about Mongo Connector?
Your question or problem may be answered in the `FAQ <https://github.com/10gen-labs/mongo-connector/wiki/FAQ>`__ or in the `wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.
If you can't find the answer to your question or problem there, feel free to `open an issue <https://github.com/10gen-labs/mongo-connector/issues>`__ on Mongo Connector's Github page.
