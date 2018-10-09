===============
mongo-connector
===============

Ownership of the mongo-connector repo has been transferred to YouGov.  MongoDB would like to thank
Jason Coombs, Executive Technical Director at YouGov, for agreeing to maintain this project.

.. image:: https://travis-ci.org/mongodb-labs/mongo-connector.svg?branch=master
   :alt: View build status
   :target: https://travis-ci.org/mongodb-labs/mongo-connector

For complete documentation, check out the `Mongo Connector Wiki
<https://github.com/yougov/mongo-connector/wiki>`__.

System Overview
---------------

`mongo-connector` creates a pipeline from a MongoDB cluster to one or more
target systems, such as Solr, Elasticsearch, or another MongoDB cluster.  It
synchronizes data in MongoDB to the target then tails the MongoDB oplog, keeping
up with operations in MongoDB in real-time. Detailed documentation is
available on the `wiki
<https://github.com/yougov/mongo-connector/wiki>`__.

Getting Started
---------------

mongo-connector supports Python 2.6, 2.7, and 3.3+ and MongoDB versions
2.4, 2.6, 3.0, 3.2, and 3.4.

Installation
~~~~~~~~~~~~

To install mongo-connector with the MongoDB doc manager suitable for
replicating data to MongoDB, use `pip <https://pypi.python.org/pypi/pip>`__::

  pip install mongo-connector


The install command can be customized to include the `Doc Managers`_
and any extra dependencies for the target system.

+----------------------------------+-------------------------------------------------+
|         Target System            |            Install Command                      |
+==================================+=================================================+
| MongoDB                          | ``pip install mongo-connector``                 |
+----------------------------------+-------------------------------------------------+
| Elasticsearch 1.x                | ``pip install 'mongo-connector[elastic]'``      |
+----------------------------------+-------------------------------------------------+
| Amazon Elasticsearch 1.x Service | ``pip install 'mongo-connector[elastic-aws]'``  |
+----------------------------------+-------------------------------------------------+
| Elasticsearch 2.x                | ``pip install 'mongo-connector[elastic2]'``     |
+----------------------------------+-------------------------------------------------+
| Amazon Elasticsearch 2.x Service | ``pip install 'mongo-connector[elastic2-aws]'`` |
+----------------------------------+-------------------------------------------------+
| Elasticsearch 5.x                | ``pip install 'mongo-connector[elastic5]'``     |
+----------------------------------+-------------------------------------------------+
| Solr                             | ``pip install 'mongo-connector[solr]'``         |
+----------------------------------+-------------------------------------------------+

You may have to run ``pip`` with ``sudo``, depending
on where you're installing mongo-connector and what privileges you have.

Development
~~~~~~~~~~~

You can also install the development version of mongo-connector
manually::

  git clone https://github.com/yougov/mongo-connector.git
  pip install ./mongo-connector

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
You can also use mongo-connector with a `configuration file <https://github.com/yougov/mongo-connector/wiki/Configuration-File>`__.

If you want to jump-start into using mongo-connector with a another particular system, check out:

- `Usage with Solr <https://github.com/yougov/mongo-connector/wiki/Usage%20with%20Solr>`__
- `Usage with Elasticsearch <https://github.com/yougov/mongo-connector/wiki/Usage%20with%20ElasticSearch>`__
- `Usage with MongoDB <https://github.com/yougov/mongo-connector/wiki/Usage%20with%20MongoDB>`__

Doc Managers
~~~~~~~~~~~~

Elasticsearch 1.x: https://github.com/mongodb-labs/elastic-doc-manager

Elasticsearch 2.x and 5.x: https://github.com/mongodb-labs/elastic2-doc-manager

Solr: https://github.com/mongodb-labs/solr-doc-manager

The MongoDB doc manager comes packaged with the mongo-connector project.

Troubleshooting/Questions
-------------------------

Having trouble with installation? Have a question about Mongo Connector?
Your question or problem may be answered in the `FAQ <https://github.com/yougov/mongo-connector/wiki/FAQ>`__
or in the `wiki <https://github.com/yougov/mongo-connector/wiki>`__. If you can't find the answer to your question or problem there, feel free to `open an issue
<https://github.com/yougov/mongo-connector/issues>`__ on Mongo Connector's Github page.
