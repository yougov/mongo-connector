"""Utilities for maintaining portability between various Python versions"""

import sys

PY3 = (sys.version_info[0] == 3)

if PY3:
    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)
    from itertools import zip_longest
else:
    exec("""def reraise(exctype, value, trace=None):
    raise exctype, str(value), trace
""")
    from itertools import izip_longest as zip_longest

if PY3:
    from urllib.request import Request
    from urllib.request import urlopen
    from urllib.error import URLError
    from urllib.error import HTTPError
else:
    from urllib2 import Request
    from urllib2 import urlopen
    from urllib2 import URLError
    from urllib2 import HTTPError
