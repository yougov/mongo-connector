## System Overview:

The mongo-connector system is designed to hook up mongoDB to any backend. This allows all the
documents in mongoDB to be stored in some other system, and both mongo and the backend will remain
in sync while the connector is running.

## Usage:

Since the connector does real time syncing, it is necessary to have MongoDB running, although the
connector will work with both sharded and non sharded configurations. To start the system, first
move your doc manager file, or one of the sample doc manager files provided to the main folder
(mongo-connector) and rename it doc_manager.py. For more information about making your own
doc manager, see Doc Manager section.
After that, simply run "python mongo_connector.py". It is likely, however, that you will need
to specify some command line options to work with your setup. They are described below:

`-m` or `--mongos` is to specify the mongos address, which is a host:port pair, or for clusters with
 one shard, the primary's address. For example, `-m localhost:27217` would be a valid argument
 to `-m`.

`-b` or `--backend-url` is to specify the URL to the backend engine being used. For example, if you
were using Solr out of the box, you could use "-b http://localhost:8080/solr" with the
SolrDocManager to establish a proper connection.

`-o` or `--oplog-ts` is to specify the name of the file that stores the oplog progress timestamps.
This file is used by the system to store the last timestamp read on a specific oplog. This allows
for quick recovery from failure. By default this is config.txt, which starts off empty. An empty
file causes the system to go through all the mongo oplog and sync all the documents.

`-n` or `--namespace-set` is used to specify the namespaces we want to consider. For example, if we
wished to store all documents from the test.test and alpha.foo namespaces, we could use
`-n test.test,alpha.foo`. The default is to consider all the namespaces, excluding the system and config
databases, and also ignoring the "system.indexes" collection in any database.

`-u` or `--unique-key` is used to specify the uniqueKey used by the backend. The default is "_id",
which can be noted by "-u _id"

`-k` or `--keyFile` is used to specify the path to the authentication key file. This file is used
by mongos to authenticate connections to the shards, and we'll use it in the oplog threads. If
authentication is not used, then this field can be left empty as the default is None.

An example of combining all of these is:

	python mongo_connector.py -m localhost:27217 -b http://localhost:8080/solr -o oplog_progress.txt -n alpha.foo,test.test -u _id -k auth.txt

## Usage With Solr:

We have provided an example Solr schema called `schema.xml`, which provides field definitions for the 'name', '_ts', `ns`, and `_id` fields. The schema also sets the `_id` field to be the unique key by adding this line:

     <uniqueKey>_id</uniqueKey>

Solr does not require all the fields to be present, unless a field's `required` attribute is `True`, but the schema must have a line defining every field that may be present in a document field. Some examples of field definitions in the schema are:

    <field name="_id" type="string" indexed="true" stored="true" />
    <field name="_ts" type="long" indexed="true" stored="true" />

For a more complete guide to adding fields, review the Solr documentation.

## DocManager

This is the only file that is engine specific. In the current version, we have provided sample
documents for ElasticSearch and Solr. If you would like to integrate MongoDB with some other engine,
then you need to write a doc_manager.py file for that backend. The sample_doc_manager.py file gives a
detailed description of the functions used by the Doc Manager, and the following functions must be
implemented:

__1) init(self, url)__

This method may vary from implementation to implementation, but it must
verify the url to the backend and return None if that fails. It must
also create the connection to the backend, and start a periodic
committer if necessary. It can take extra optional parameters for internal use, like
auto_commit.

__2) upsert(self, doc)__

Update or insert a document into your engine.
This method should call whatever add/insert/update method exists for
the backend engine and add the document in there. The input will
always be one mongo document, represented as a Python dictionary.

__3) remove(self, doc)__

Removes documents from engine
The input is a python dictionary that represents a mongo document.

__4) search(self, start_ts, end_ts)__

Called to query engine for documents in a time range, including start_ts and end_ts
This method is only used by rollbacks to query all the documents in
Solr within a certain timestamp window. The input will be two longs
(converted from Bson timestamp) which specify the time range. The
return value should be an iterable set of documents.


__5) commit(self)__

This function is used to force a refresh/commit.
It is used only in the beginning of rollbacks and in test cases, and is
not meant to be called in other circumstances. The body should commit
all documents to the backend engine (like auto_commit), but not have
any timers or run itself again (unlike auto_commit).

__6) get_last_doc(self)__

Returns the last document stored in the Elastic engine.
This method is used for rollbacks to establish the rollback window,
which is the gap between the last document on a mongo shard and the
last document in Solr. If there are no documents, this functions
returns None. Otherwise, it returns the first document.


## System Internals:

The main Connector thread connects to either a mongod or a mongos, depending on cluster setup, and
spawns an OplogThread for every primary node in the cluster. These OplogThreads continuously poll
the Oplog for new operations, get the relevant documents, and insert and/or update them into the
backend system. The general workflow of an OplogThread is:

1. "Prepare for sync", which initializes a tailable cursor to the Oplog of the mongod. This cursor
    will return all new entries, which are processed in step 2. If this is the first time the
    mongo-connector is being run, then a special "dump collection" occurs where all the documents
    from a given namespace are dumped into the backend. On subsequent calls, the timestamp of the
    last oplog entry is stored in the system, so the OplogThread resumes where it left off instead
    of rereading  the whole oplog.

2. For each entry in the oplog, we see if it's an insert/update operation or a delete operation.
   For insert/update operations, we fetch the document from either the mongos or the primary
   (depending on cluster setup). This document is passed up to the DocManager. For deletes, we pass
   up the unique key for the document to the DocManager.

3. The DocManager can either batch documents and periodically update the backend, or immediately,
   or however the user chooses to.

The above three steps essentially loop forever. For usage purposes, the only relevant layer is the
DocManager, which will always get the documents from the underlying layer and is responsible for
adding to/removing from the backend.

Mongo-Connector imports a DocManager from the file doc_manager.py. We have provided sample
implementations for a Solr search DocManager and an ElasticSearch DocManager, but for
Mongo-Connector to use these, it is necessary to copy the contents to the doc_manager.py file. So,
if you wish to use the Solr manager, you could execute 'cp solr_doc_manager.py doc_manager.py' and
then start the connector.

## Testing scripts

There are two sets of tests - one is for the general system, which can be found in the Tests folder, and another is for
the Doc Managers, which is found in the tests folder inside the doc_managers folder. The doc manager tests utilize
the Doc Managers stored in the doc_managers folder, so if any modifications are made to a Doc Manager, ensure that the
changes are propogated in the corresponding file in the DocManagers folder. That is, while the actual running
system will use the doc_manager.py file in the main mongo-connector folder, the tests use the files in the
doc_managers folder.
There are shell scripts for running all tests for each search engine (currenctly Solr and Elastic) in doc_managers/tests;
for the general connector tests in /tests, and a script that runs all tests in the main folder.
