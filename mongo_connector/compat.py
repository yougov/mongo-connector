"""Deprecated compat library, retained for doc managers"""

import sys
import warnings

from urllib.request import Request
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import URLError
from urllib.error import HTTPError

__all__ = [
    'Request', 'urlopen', 'urlencode', 'URLError', 'HTTPError',
    'reraise', 'is_string', 'u', 'PY3',
]

warnings.warn(
    "mongo_connector.compat should not be used",
    DeprecationWarning,
)

PY3 = sys.version_info[0] == 3


def reraise(exctype, value, trace=None):
    raise exctype(str(value)).with_traceback(trace)


def is_string(x):
    return isinstance(x, str)


u = str
