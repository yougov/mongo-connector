# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys

from mongo_connector.compat import reraise


def exception_wrapper(mapping):
    def decorator(f):
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                exc_type, exc_value, exc_tb = sys.exc_info()
                new_type = mapping.get(exc_type)
                if new_type is None:
                    raise
                reraise(new_type, exc_value, exc_tb)
        return wrapped
    return decorator


class DocManagerBase(object):
    """Base class for all DocManager implementations."""

    def apply_update(self, doc, update_spec):
        """Apply an update operation to a document."""
        doc.update(update_spec.get("$set", {}))
        for key in update_spec.get("$unset", []):
            doc.pop(key)
        return doc

    def bulk_upsert(self, docs):
        """Upsert each document in a set of documents.

        This method may be overridden to upsert many documents at once.
        """
        for doc in docs:
            self.upsert(doc)

    def update(self, doc, update_spec):
        raise NotImplementedError

    def upsert(self, document):
        raise NotImplementedError

    def remove(self, doc):
        raise NotImplementedError

    def search(self, start_ts, end_ts):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def get_last_doc(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError
