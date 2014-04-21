System Overview
---------------

mongo-connector creates a pipeline from a MongoDB cluster to one or more
target systems, such as Solr, ElasticSearch, or another MongoDB cluster.
By tailing the MongoDB oplog, it replicates operations from MongoDB to
these systems in real-time. It has been tested with Python 2.6, 2.7,
3.3, and 3.4. Detailed documentation is available on the
`wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.

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
                  -d <path to DocManager, e.g. doc_managers/solr_doc_manager.py>

mongo-connector has many other options besides those demonstrated above.
To get a full listing with descriptions, try ``mongo-connector --help``.

Usage With Solr
---------------

There is an example Solr schema called
`schema.xml <https://github.com/10gen-labs/mongo-connector/blob/master/mongo_connector/doc_managers/schema.xml>`__,
which provides several field definitions on which mongo-connector
relies, including:

-  ``_id``, the default unique key for documents in MongoDB (this may be
   changed with the ``--unique-key`` option)
-  ``ns``, the namespace from which the document came
-  ``_ts``, the timestamp from the oplog entry that last modified the
   document

The sample XML schema is designed to work with the tests. For a more
complete guide to adding fields, review the `Solr
documentation <http://wiki.apache.org/solr/SchemaXml>`__.

You may also want to jump to the mongo-connector `Solr
wiki <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20Solr>`__
for more detailed information on using mongo-connector with Solr.

Troubleshooting
---------------

**Installation**

Some users have experienced trouble installing mongo-connector, noting
error messages like the following::

  Processing elasticsearch-0.4.4.tar.gz
  Running elasticsearch-0.4.4/setup.py -q bdist_egg --dist-dir /tmp/easy_install-gg9U5p/elasticsearch-0.4.4/egg-dist-tmp-vajGnd
  error: /tmp/easy_install-gg9U5p/elasticsearch-0.4.4/README.rst: No such file or directory

The workaround for this is making sure you have a recent version of
``setuptools`` installed. Any version *after* 0.6.26 should do the
trick::

  pip install --upgrade setuptools

**Running mongo-connector after a long time**

If you see a message like this from mongo-connector::

  ERROR - OplogManager: Last entry no longer in oplog cannot recover! ...

then mongo-connector may have fallen behind in the oplog, and
discrepencies must now be resolved between the contents of the target
system and those in MongoDB. If you're just playing around with
mongo-connector, however, then you may have stopped mongo-connector,
made a bunch of requests to MongoDB or perhaps started a new replica
set, then restarted mongo-connector, which will also cause this issue.
In the latter case, all you need to do is use a new ``--oplog-ts`` file
or erase the old one.
