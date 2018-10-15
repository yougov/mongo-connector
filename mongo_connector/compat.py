"""Utilities for maintaining portability between various Python versions"""

# flake8: noqa

import sys
import warnings

PY3 = sys.version_info[0] == 3

if PY3:

    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)

    def is_string(x):
        return isinstance(x, str)

    from urllib.request import Request
    from urllib.request import urlopen
    from urllib.parse import urlencode
    from urllib.error import URLError
    from urllib.error import HTTPError

    def u(s):
        return str(s)


else:
    warnings.warn(
        "Python 2 support is deprecated and pending removal. Please "
        "run mongo-connector on Python 3. See "
        "https://github.com/yougov/mongo-connector/issues/829 "
        "for more details or to post concerns."
    )
    exec(
        """def reraise(exctype, value, trace=None):
    raise exctype, value, trace"""
    )

    def is_string(x):
        return isinstance(x, basestring)

    from urllib import urlencode
    from urllib2 import Request
    from urllib2 import urlopen
    from urllib2 import URLError
    from urllib2 import HTTPError

    def u(s):
        return unicode(s)
