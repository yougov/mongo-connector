import inspect
import time
import logging
from mongo_connector.doc_managers.mongo_doc_manager import DocManager as MongoDocManager
from random import random
import pymongo
from mongo_connector import errors, constants
from mongo_connector.util import exception_wrapper

wrap_exceptions = exception_wrapper({
    pymongo.errors.ConnectionFailure: errors.ConnectionFailed,
    pymongo.errors.OperationFailure: errors.OperationFailed})

LOG = logging.getLogger(__name__)


def request_rate_retry(f):
    def wrapped(*args, **kwargs):
        s = 1 + random()
        attempt = 0
        while True:
            try:
                return f(*args, **kwargs)
            except Exception, e:
                if "request rate is" in str(e.message).lower():
                    LOG.info("Too fast for CosmosDB, Attempt %d: '%s', sleeping for %.2f sec" % (attempt, e, s))
                    time.sleep(s)
                    if s < 30:
                        s = s * 2 + random()
                    attempt += 1
                else:
                    raise

    return wrapped


def for_all_methods(decorator):
    def decorate(cls):
        for name, fn in inspect.getmembers(cls, inspect.ismethod):
            setattr(cls, name, decorator(fn))
        return cls

    return decorate


@for_all_methods(request_rate_retry)
class DocManager(MongoDocManager):
    def __init__(self, url, **kwargs):
        super(DocManager, self).__init__(url, **kwargs)
        self.timestamp_field = kwargs.get('timestamp_field', '')

    def _is_doc_ok(self, doc):
        if len(str(doc)) > 2 * 1024 * 1024:
            LOG.warn('Document too large (%d bytes), skipping: %s....%s' % (len(str(doc)), str(doc)[:5000], str(doc)[-5000:]))
            return False
        return True

    def handle_command(self, doc, namespace, timestamp):
        if not self._is_doc_ok(doc):
            return
        super(DocManager, self).handle_command(doc, namespace, timestamp)

    def update(self, document_id, update_spec, namespace, timestamp):
        if not self._is_doc_ok(update_spec):
            return
        super(DocManager, self).update(document_id, update_spec, namespace, timestamp)

    def upsert(self, doc, namespace, timestamp):
        if not self._is_doc_ok(doc):
            return
        if not self.timestamp_field == '':
            doc[self.timestamp_field] = timestamp
        super(DocManager, self).upsert(doc, namespace, timestamp)

    def bulk_upsert(self, docs, namespace, timestamp):
        def iterate_chunks():
            dbname, collname = self._db_and_collection(namespace)
            collection = self.mongo[dbname][collname]
            meta_collection_name = self._get_meta_collection(namespace)
            meta_collection = self.meta_database[meta_collection_name]
            more_chunks = True
            while more_chunks:
                bulk = collection.initialize_ordered_bulk_op()
                bulk_meta = meta_collection.initialize_ordered_bulk_op()
                for i in range(self.chunk_size):
                    try:
                        doc = next(docs)
                        if not self._is_doc_ok(doc):
                            continue
                        selector = {'_id': doc['_id']}
                        bulk.find(selector).upsert().replace_one(doc)
                        meta_selector = {self.id_field: doc['_id']}
                        bulk_meta.find(meta_selector).upsert().replace_one({
                            self.id_field: doc['_id'],
                            'ns': namespace,
                            '_ts': timestamp
                        })
                    except StopIteration:
                        more_chunks = False
                        if i > 0:
                            yield bulk, bulk_meta
                        break
                if more_chunks:
                    yield bulk, bulk_meta

        for bulk_op, meta_bulk_op in iterate_chunks():
            try:
                try:
                    request_rate_retry(bulk_op.execute)()
                    request_rate_retry(meta_bulk_op.execute)()
                except:
                    LOG.error("bulk_op was: %r" % (bulk_op._BulkOperationBuilder__bulk.ops,))
                    LOG.error("meta_bulk_op was: %r" % (meta_bulk_op._BulkOperationBuilder__bulk.ops,))
                    raise
            except pymongo.errors.DuplicateKeyError as e:
                LOG.warn('Continuing after DuplicateKeyError: '
                         + str(e))
            except pymongo.errors.BulkWriteError as bwe:
                LOG.error(bwe.details)
                raise e