## System Overview:

The mongo-connector system is designed to hook up mongoDB to any target system. This allows all the
documents in mongoDB to be stored in some other system, and both mongo and the target system will remain
in sync while the connector is running. It has been tested with python 2.7 and python 3.

## Getting Started:

Since the connector does real time syncing, it is necessary to have MongoDB running, although the
connector will work with both sharded and non sharded configurations. It requires a replica set
setup.

To start the system, simply run "python mongo_connector.py". It is likely, however, that you will need
to specify some command line options to work with your setup. They are described below:

`-m` or `--mongos` is to specify the main address, which is a host:port pair. For sharded clusters, 
this should be the mongos address. For individual replica sets, supply the address of the primary.
For example, `-m localhost:27217` would be a valid argument to `-m`. Don't use quotes around the address.

`-t` or `--target-url` is to specify the URL to the target system being used. For example, if you
were using Solr out of the box, you could use '-t http://localhost:8080/solr' with the
SolrDocManager to establish a proper connection. Don't use quotes around address. If target
system doesn't need URL, don't specify.

`-o` or `--oplog-ts` is to specify the name of the file that stores the oplog progress timestamps.
This file is used by the system to store the last timestamp read on a specific oplog. This allows
for quick recovery from failure. By default this is `config.txt`, which starts off empty. An empty
file causes the system to go through all the mongo oplog and sync all the documents. Whenever the
cluster is restarted, it is essential that the oplog-timestamp config file be emptied - otherwise
the connector will miss some documents and behave incorrectly.

`-n` or `--namespace-set` is used to specify the namespaces we want to consider. For example, if we
wished to store all documents from the test.test and alpha.foo namespaces, we could use
`-n test.test,alpha.foo`. The default is to consider all the namespaces, excluding the system and config
databases, and also ignoring the "system.indexes" collection in any database.

`-u` or `--unique-key` is used to specify the mongoDB field that will serve
as the unique key for the target system.  The default is "_id",
which can be noted by "-u _id"

`-k` or `--keyFile` is used to specify the path to the authentication key file. This file is used
by mongos to authenticate connections to the shards, and we'll use it in the oplog threads. If
authentication is not used, then this field can be left empty as the default is None.

`-a' or `--admin-username` is used to specify the username of an admin user to authenticate with.
To use authentication with the system, the user must specify both this option and the keyFile
option, which stores the password for the user. The default username is '__system', which is not
recommended for production use.

`-d` or `--docManager` is used to specify the doc manager file that is going to be used.
You should send the path of the file you want to be used. By default, it will use
the doc_manager_simulator.py file. It is recommended that all doc manager files be kept
in the doc_managers folder in mongo-connector.
For more information about making your own doc manager, see Doc Manager section.

An example of combining all of these is:

	python mongo_connector.py -m localhost:27217 -b http://localhost:8080/solr -o oplog_progress.txt -n alpha.foo,test.test -u _id -k auth.txt -a admin -d ./doc_managers/solr_doc_manager.py

## Usage With Solr:

We have provided an example Solr schema called `schema.xml`, which provides field definitions for the 'name', '_ts', `ns`, and `_id` fields. The schema also sets the `_id` field to be the unique key by adding this line:

     <uniqueKey>_id</uniqueKey>

Solr does not require all the fields to be present, unless a field's `required` attribute is `True`, but the schema must have a line defining every field that may be present in a document field. Some examples of field definitions in the schema are:

    <field name="_id" type="string" indexed="true" stored="true" />
    <field name="_ts" type="long" indexed="true" stored="true" />

The sample XML schema is designed to work with the tests. For a more complete guide to adding fields, review the Solr documentation.

## DocManager

This is the only file that is engine specific. In the current version, we have provided sample
implementations for ElasticSearch, Solr and Mongo, which are in the docmanagers folder.
If you would like to integrate MongoDB with some other engine, then you need to write a
doc manager file for that target system. The sample_doc_manager.py file gives a
detailed description of the functions used by the Doc Manager; the following functions must be
implemented:

__1) init(self)__

This method should if needed, verify the url to the target system and return None if that fails. It should
also create the connection to the target system, and start a periodic
committer if necessary. It can take extra optional parameters for internal use, like
auto_commit.
The unique_key should default to '_id' and it is an obligatory parameter.
It requires a url paramater iff mongo_connector.py is called with the -b paramater.
Otherwise, it doesn't require any other parameter (e.g. if the target engine doesn't need a URL).
It should raise a SystemError exception if the URL is not valid.

__2) stop(self)__

This method must stop any threads running from the DocManager. In some cases this simply stops a
timer thread, whereas in other DocManagers it does nothing because the manager
doesn't use any threads. This method is only called when the MongoConnector is
forced to terminate, either due to errors or as part of normal procedure.

__3) upsert(self, doc)__

Update or insert a document into your engine. The document has ns and _ts fields.
This method should call whatever add/insert/update method exists for
the target system engine and add the document in there. The input will
always be one mongo document, represented as a Python dictionary.
This document will be the current mongo version of the document,
not necessarily the version at the time the upsert was made; the
doc manager will be responsible to track the changes if necessary.
Note this is not necessary to ensure consistency.
It is possible to get two inserts for the same document with the same
contents if there is considerable delay in trailing the oplog.
We have only one function for update and insert because incremental
updates are not supported, so there is no update option.

__4) remove(self, doc)__

Removes document from engine
The input is a python dictionary that represents a mongo document.

__5) search(self, start_ts, end_ts)__

Called to query engine for documents in a time range, including start_ts and end_ts
This method is only used by rollbacks to query all the documents in
the target engine within a certain timestamp window. The input will be two longs
(converted from Bson timestamp) which specify the time range. The 32 most significant
bits are the Unix Epoch Time, and the other bits are the increment. For all purposes,
the function should just do a simple search for timestamps between these values
treating them as simple longs.
The return value should be an iterable set of documents.

__6) commit(self)__

This function is used to force a refresh/commit.
It is used only in the beginning of rollbacks and in test cases, and is
not meant to be called in other circumstances. The body should commit
all documents to the target system (like auto_commit), but not have
any timers or run itself again (unlike auto_commit).  In the event of
too many engine searchers, the commit can be wrapped in a
retry_until_ok to keep trying until the commit goes through.

__7) get_last_doc(self)__

Returns the last document stored in the target engine.
This method is used for rollbacks to establish the rollback window,
which is the gap between the last document on a mongo shard and the
last document. If there are no documents, this functions
returns None. Otherwise, it returns the first document.


## System Internals:

The main Connector thread connects to either a mongod or a mongos, depending on cluster setup, and
spawns an OplogThread for every primary node in the cluster. These OplogThreads continuously poll
the Oplog for new operations, get the relevant documents, and insert and/or update them into the
target system. The general workflow of an OplogThread is:

1. "Prepare for sync", which initializes a tailable cursor to the Oplog of the mongod. This cursor
    will return all new entries, which are processed in step 2. If this is the first time the
    mongo-connector is being run, then a special "dump collection" occurs where all the documents
    from a given namespace are dumped into the target system. On subsequent calls, the timestamp of the
    last oplog entry is stored in the system, so the OplogThread resumes where it left off instead
    of rereading  the whole oplog.

2. For each entry in the oplog, we see if it's an insert/update operation or a delete operation.
   For insert/update operations, we fetch the document from either the mongos or the primary
   (depending on cluster setup). This document is passed up to the DocManager. For deletes, we pass
   up the unique key for the document to the DocManager.

3. The DocManager can either batch documents and periodically update the target system, or immediately,
   or however the user chooses to.

The above three steps essentially loop forever. For usage purposes, the only relevant layer is the
DocManager, which will always get the documents from the underlying layer and is responsible for
adding to/removing from the target system.

Mongo-Connector imports a DocManager from the specified file in mongo_connector.py, preferably
from the doc_managers folder. We have provided sample implementations for a Solr search DocManager
 and an ElasticSearch DocManager. Note that upon execution, mongo_connector.py will copy the file
 to the main folder as doc_manager.py.
 
The documents stored in the target system are equivalent to what is stored in mongo, except every
document has two additional fields called `ns` and `_ts`. The `_ts` field stores the latest
timestamp corresponding to that document in the oplog. For example, if document `D` was inserted
at time `t1` and then updated at time `t2`, then after processing the update, we update the
documents timestamp to reflect the most recent time. This is relevant for rollbacks in the system.

The `ns` field stores the namespace for each document. This is relevant to prevent conflicts
between identical documents in different namespaces, and also used for managing rollbacks and
syncing.

## Testing scripts

There are two sets of tests. One is for the general system, which can be found in the Tests folder, and the other is for
the Doc Managers, which is found in the tests folder inside the doc_managers folder. The doc manager tests use
the Doc Managers stored in the doc_managers folder; unlike mongo_connector.py they do not copy the file to
doc_manager.py in the main folder.

There are shell scripts for running all tests for each search engine (currenctly Solr, Elastic and Mongo),
for the general connector tests, and a script that runs all tests. You can find them in tests/scripts.

A word of caution: For some of the tests, specifically test_oplog_manager, the output prints out "ERROR:root:OplogThread:
No oplog for thread: Connection('localhost', 27117)". This is expected behavior because the tests run some parts in isolation,
and are not an indication of error. If the tests actually fail, a distinct message will be printed at the very end stating exactly where it failed.

## Troubleshooting

The most common issue when installing is to forget to clear config.txt before restarting the mongo connector.
This config.txt file stores an oplog timestamp which indicates at which point of the oplog the connector should
start. This is made so you can stop syncing at any point by quitting the program and restart from where you left
off later on.
However, this can be a problem if you have restarted mongo and that timestamp happens before all entries in the
oplog; this will result in an error, since the system will realize that you have a stale oplog timestamp. 
Additionally, if you restart mongo, the target system will not have the data automatically wiped. This is
of course intentional, since it would be undesirable to have the data disappear from the target system if
mongo stops working for whatever reason.
