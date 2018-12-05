Changelog
=========

Version 3.1.1
-------------

- #836: Remove $v entry in update. Improves support for MongoDB 3.6.

Version 3.1.0
-------------

- #772: Allow for exit after dump.

Version 3.0.1
-------------

- #843: Restored ``mongo_connector.compat`` for compatibility in
  the doc managers.

Version 3.0.0
-------------

- Dropped support for Python 3.3 and earlier.
- Dropped support for EOL MongoDB 3.2 and earlier.
- Moved System V Init script installation and uninstallation to a
  runpy module.

Version 2.7.0
-------------

- #829: mongo-connector now emits a warning when running under
  Python 2.

Version 2.6.0
-------------

- Project maintenance transferred to YouGov.
- #816: Added utility for getting oplog timestamp from replica set.
- #826: Project now automatically releases tagged commits to PyPI via
  Travis CI.
- #781: Improved MongoDB 3.6 support by removing ``$v``.

Version 2.5.1
-------------

Version 2.5.1 improves testing, documentation, and fixes the following bugs:

- Only use listDatabases when necessary.
- Do not use the listShards command.
- Fix PyMongo 3.0 compatibility.
- Fixes support for MongoDB 2.4's invalid $unsets operations.
- Set array element to null when $unset, do not remove the element completely.
- Command line SSL options should override the config file.
- Properly send "ssl.sslCertificatePolicy" to MongoClients.
- Properly output log messages while configuration is parsed.
- All source clients should inherit MongoDB URI options from the main address.
- Do not retry operations that result in authorization failure.

Version 2.5.0
-------------

Version 2.5.0 adds some new features, bug fixes, and minor breaking changes.

New Features
~~~~~~~~~~~~

- Support for MongoDB 3.4.
- Support including or excluding fields per namespace.
- Support wildcards (*) in namespaces.
- Support for including and excluding different namespaces at the same time.
- Adds a new config file format for the `namespaces` option.
- Logs environment information on startup.
- The doc managers can now be installed through extras_require with pip.
- mongo-connector now tests against MongoDB versions 2.4, 2.6, 3.0, 3.2, and 3.4.


Bug Fixes
~~~~~~~~~

- mongo-connector now gracefully exits on SIGTERM.
- Improved handling of rollbacks.
- Now handles mongos connection failure while looking for shards.
- mongo-connector can now be cancelled during the initial collection dump.
- Improved handling of connection failure while tailing the oplog.
- Command line doc manager specific options now override the config file.
- Improved filtering of nested fields.

Breaking Changes
~~~~~~~~~~~~~~~~

- The solr-doc-manager has been extracted into a separate package and is not
  included by default. See https://pypi.python.org/pypi/solr-doc-manager.
- Asterisks (*) in namespaces configuration are now interpreted as wildcards.

Version 2.4.1
-------------

- Collection dump does not require namespace-set collection entries to be in the oplog.
- Fix an issue where a rollback was misinterpreted as falling off the oplog.
- Fix issue when re-raising exceptions containing unicode.

Version 2.4
-----------

- Add --exclude-fields option.
- Better handling of exceptions in doc managers.
- Allow doc managers to be imported from anywhere, given the full path.
- Do not call count() on oplog cursors.
- Change the oplog format to be resilient to replica set failover.

.. warning:: The change to the oplog timestamp file format means that downgrading
             from this version is not possible!

Version 2.3
-----------

- Make self._fields in OplogThread a set.
- Move elastic doc managers into their own projects.

Version 2.2
-----------
- Support for using a single meta collection to track replication to MongoDB cluster
- The log format is now configurable
- Bug fix for using '—no-dump' when nothing is in the oplog
- Now requires PyMongo 2.9+

Version 2.1
-----------

Version 2.1 adds a couple minor features and fixes a few bugs.

New Features
~~~~~~~~~~~~

- Bulk write support when synchronizing to MongoDB.
- Add 'docManagers.XXX.args.clientOptions' to the config file for passing arbitrary keyword arguments to clients contained within a DocManager.
- Add 'docManagers.XXX.bulkSize' to the config file for adjusting the size of bulk requests.
- Add '--stdout' option to mongo-connector for printing logs to STDOUT.

Bug Fixes
~~~~~~~~~

- Filter replacement documents in filter_oplog_entry.
- Multiple improvements to the test suite to stand up better across various versions of MongoDB.
- Authenticate to shards within a sharded cluster (thanks to Hugo Hromic!)
- Raise InvalidConfiguration for unrecognized command-line arguments.
- Clean out 'NaN'/'inf' from number fields in Elasticsearch (thanks to jaredkipe!)
- Fix autoCommitInterval when set to 0.

Version 2.0.3
-------------

Version 2.0.3 requires that the PyMongo version installed be in the range [2.7.2, 3.0). It also adds more fine-grained control over log levels.

Version 2.0.2
-------------

Version 2.0.2 fixes the following issues:

- Fix configuring timezone-aware datetimes (--tz-aware).
- Fix password file from the command line (--password-file).
- Automatically escape certain characters from field names in documents sent to Solr.
- Add a lot more testing around the configuration file and command-line options.

Version 2.0.1
-------------

Version 2.0.1 fixes filtering by namespace (--namespace-set, namespaces.include).

Version 2.0
-----------

Version 2.0 is a major version of Mongo Connector and includes breaking changes, new features, and bug fixes.

Improvements
~~~~~~~~~~~~

- SSL certificates may now be given to Mongo Connector to validate connections to MongoDB.
- A new JSON configuration file makes configuring and starting Mongo Connector as a system service much easier.
- The `setup.py` file can now install Mongo Connector as a service automatically.
- Support for replicating files in GridFS.
- Allow DocManagers to be distributed as separate packages, rather than needing a fork or pull request.
- DocManagers may handle arbitrary database commands in the oplog.

Bug Fixes
~~~~~~~~~

- Adding an element beyond the end of an array in MongoDB no longer throws an exception.
- All errors that cause Mongo Connector to exit are written to the log.
- Automatically use all-lowercase index names when targeting Elasticsearch.

Breaking Changes
~~~~~~~~~~~~~~~~

- The constructor signatures for OplogThread and Connector have changed:
        - The `u_key` and `target_url` keyword arguments have been removed from the constructor for Connector.
        - `target_url` is gone from the OplogThread constructor.
        - The `doc_manager` keyword argument in the constructors for Connector and OplogThread is now called `doc_managers`.
        - The `doc_managers` keyword argument in Connector takes a list of **instances** of `DocManager`, rather that a list of strings corresponding to files that define DocManagers.
- ConnectorError has been removed. Exceptions that occur when constructing Connector will be passed on to the caller.
- The DocManagerBase class moved from mongo_connector.doc_managers to mongo_connector.doc_managers.doc_manager_base
- The exception_wrapper function moved from mongo_connector.doc_managers to mongo_connector.util
- The arguments to many DocManager methods have changed. For an up-to-date overview of how to write a custom DocManager, see the `Writing Your Own DocManager wiki page <https://github.com/10gen-labs/mongo-connector/wiki/Writing-Your-Own-DocManager>`__. A synopsis:
        - The `remove` method now takes a document id, namespace, and a timestamp instead of a whole document.
        - The `upsert`, `bulk_upsert`, and `update` methods all take two additional arguments: namespace and timestamp.

Version 1.3.1
-------------

Version 1.3.1 contains mostly bug fixes and adds timezone-aware timestamp support. Bugs fixed include:

- Fixes for update operations to Solr.
- Re-insert documents that were deleted before a rollback.
- Catch a few additional exceptions sometimes thrown by the Elasticsearch Python driver.


Version 1.3
-----------

Version 1.3 fixes many issues and adds a couple minor features. Highlights include:

- Use proper updates instead of upserting the most recent version of a document.

.. Warning:: Update operations require ``_source`` field to be enabled in Elasticsearch.

- Fix many issues relating to sending BSON types to external drivers, such as for Elasticsearch and Solr.
- Fix several issues related to using a unique key other than ``_id``.
- Support all UTF8 database and collection names.
- Keep namespace and timestamp metadata in a separate Elasticsearch index.
- Documentation overhaul for using Mongo Connector with Elasticsearch.
- New ``--continue-on-error`` flag for collection dumps.
- ``_id`` is no longer duplicated in ``_source`` field in Elasticsearch.

Version 1.2.1
-------------

Version 1.2.1 fixes some trivial installation issues and renames the CHANGELOG to CHANGELOG.rst.

Version 1.2
-----------

Version 1.2 is a major release with a large number of fixes since the last release on PyPI. It also includes a number of improvements for use with Solr and ElasticSearch.

Improvements
~~~~~~~~~~~~

- Ability to have multiple targets of replication
- Ability to upsert documents containing arrays and nested documents with the Solr DocManager
- Upserts during a collection dump may happen in bulk, resulting in a performance boost
- mongo-connector does not commit writes in target systems by default, resulting in a peformance boost

.. Warning:: This new behavior may give unexpected delays before
             documents are comitted in the target system. Most
             indexing systems provide some way of configuring how
             often changes should be comitted. Please see the relevant
             wiki articles for `Solr
             <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20Solr#managing-commit-behavior/>`_
             and `ElasticSearch
             <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20ElasticSearch#managing-refresh-behavior/>`_
             for more information on configuring commit behavior for
             your system. Note that MongoDB as a target system is
             unaffected by this change.

- Addition of ``auto-commit-interval`` to the command-line options
- Ability to change the destination namespace of upserted documents
- Ability to restrict the fields upserted in documents
- Memory footprint reduced
- Collection dumps may happen in batch, resulting in huge performance gains

Fixes
~~~~~

- Fix for unexpected exit during chunk migrations and orphan documents in MongoDB
- Fix installation problems due to namespace issues

.. Warning:: RENAME of ``mongo_connector.py`` module to
             ``connector.py``. Thus, if you should need to import the
             ``Connector`` object, you now should do
             ``from mongo_connector.connector import Connector``

- Fix user-specified unique keys in Solr and ElasticSearch DocManagers
- Fix for keyboard exit taking large amounts of time to be effective

Version 1.1.1
-------------

This was the first release of mongo-connector.
