import base64
import datetime
import re

from uuid import UUID
from math import isnan, isinf

import logging

import bson
import bson.json_util

long = int
unicode = str

LOG = logging.getLogger(__name__)

RE_TYPE = type(re.compile(""))
try:
    from bson.regex import Regex

    RE_TYPES = (RE_TYPE, Regex)
except ImportError:
    RE_TYPES = (RE_TYPE,)


class DocumentFormatter(object):
    """Interface for classes that can transform documents to conform to
    external drivers' expectations.
    """

    def transform_value(self, value):
        """Transform a leaf-node in a document.

        This method may be overridden to provide custom handling for specific
        types of values.
        """
        raise NotImplementedError

    def transform_element(self, key, value):
        """Transform a single key-value pair within a document.

        This method may be overridden to provide custom handling for specific
        types of values. This method should return an iterator over the
        resulting key-value pairs.
        """
        raise NotImplementedError

    def format_document(self, document):
        """Format a document in preparation to be sent to an external driver."""
        raise NotImplementedError


class DefaultDocumentFormatter(DocumentFormatter):
    """Basic DocumentFormatter that preserves numbers, base64-encodes binary,
    and stringifies everything else.
    """

    def transform_value(self, value):
        # This is largely taken from bson.json_util.default, though not the same
        # so we don't modify the structure of the document
        if isinstance(value, dict):
            return self.format_document(value)
        elif isinstance(value, list):
            return [self.transform_value(v) for v in value]
        if isinstance(value, RE_TYPES):
            flags = ""
            if value.flags & re.IGNORECASE:
                flags += "i"
            if value.flags & re.LOCALE:
                flags += "l"
            if value.flags & re.MULTILINE:
                flags += "m"
            if value.flags & re.DOTALL:
                flags += "s"
            if value.flags & re.UNICODE:
                flags += "u"
            if value.flags & re.VERBOSE:
                flags += "x"
            pattern = value.pattern
            # quasi-JavaScript notation (may include non-standard flags)
            return "/%s/%s" % (pattern, flags)
        elif isinstance(value, bson.Binary) or isinstance(value, bytes):
            # Just include body of binary data without subtype
            return base64.b64encode(value).decode()
        elif isinstance(value, UUID):
            return value.hex
        elif isinstance(value, (int, long, float)):
            if isnan(value):
                raise ValueError("nan")
            elif isinf(value):
                raise ValueError("inf")
            return value
        elif isinstance(value, datetime.datetime):
            return value
        elif value is None:
            return value
        # Default
        return unicode(value)

    def transform_element(self, key, value):
        try:
            new_value = self.transform_value(value)
            yield key, new_value
        except ValueError as e:
            LOG.warn("Invalid value for key: %s as %s" % (key, str(e)))

    def format_document(self, document):
        def _kernel(doc):
            for key in doc:
                value = doc[key]
                for new_k, new_v in self.transform_element(key, value):
                    yield new_k, new_v

        return dict(_kernel(document))


class DocumentFlattener(DefaultDocumentFormatter):
    """Formatter that completely flattens documents and unwinds arrays:

    An example:
      {"a": 2,
       "b": {
         "c": {
           "d": 5
         }
       },
       "e": [6, 7, 8]
      }

    becomes:
      {"a": 2, "b.c.d": 5, "e.0": 6, "e.1": 7, "e.2": 8}

    """

    def transform_element(self, key, value):
        if isinstance(value, list):
            for li, lv in enumerate(value):
                for inner_k, inner_v in self.transform_element("%s.%s" % (key, li), lv):
                    yield inner_k, inner_v
        elif isinstance(value, dict):
            formatted = self.format_document(value)
            for doc_key in formatted:
                yield "%s.%s" % (key, doc_key), formatted[doc_key]
        else:
            # We assume that transform_value will return a 'flat' value,
            # not a list or dict
            yield key, self.transform_value(value)

    def format_document(self, document):
        def flatten(doc, path):
            top_level = len(path) == 0
            if not top_level:
                path_string = ".".join(path)
            for k in doc:
                v = doc[k]
                if isinstance(v, dict):
                    path.append(k)
                    for inner_k, inner_v in flatten(v, path):
                        yield inner_k, inner_v
                    path.pop()
                else:
                    transformed = self.transform_element(k, v)
                    for new_k, new_v in transformed:
                        if top_level:
                            yield new_k, new_v
                        else:
                            yield "%s.%s" % (path_string, new_k), new_v

        return dict(flatten(document, []))
