Changelog
=========

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
