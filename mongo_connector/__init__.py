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

import logging

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)


version_info = (2, 5, 0, 'dev0')
__version__ = '.'.join(str(v) for v in version_info)


# Monkey patch logging to add Logger.always
ALWAYS = logging.CRITICAL + 10

logging.addLevelName(ALWAYS, 'ALWAYS')


def always(self, message, *args, **kwargs):
    self.log(ALWAYS, message, *args, **kwargs)

logging.Logger.always = always
