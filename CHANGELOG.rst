Changelog
=========

Version 2.0.dev0
----------------

Version 2.0.dev0 is a development version that will become version 2.0.

Breaking Changes
~~~~~~~~~~~~~~~~

- The constructor signatures for OplogThread and Connector have changed:
        - The `u_key` and `target_url` keyword arguments have been removed from the constructor for Connector.
        - `target_url` is gone from the OplogThread constructor.
        - The `doc_manager` keyword argument in the constructors for Connector and OplogThread is now called `doc_managers`.
        - The `doc_managers` keyword argument in Connector takes a list of **instances** of `DocManager`, rather that a list of strings corresponding to files that define DocManagers.
- ConnectorError has been removed. Exceptions that occur when constructing Connector will be passed on to the caller.

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
