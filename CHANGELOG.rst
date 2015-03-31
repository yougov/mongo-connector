Changelog
=========

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
----------------

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
