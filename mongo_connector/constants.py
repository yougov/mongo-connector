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


import importlib_metadata


__version__ = importlib_metadata.version("mongo_connector")


# Maximum # of documents to process before recording timestamp
# default = -1 (no maximum)
DEFAULT_BATCH_SIZE = -1

# Interval in seconds between doc manager flushes (i.e. auto commit)
# default = None (never auto commit)
DEFAULT_COMMIT_INTERVAL = None

# Maximum # of documents to send in a single bulk request through a
# DocManager.
DEFAULT_MAX_BULK = 1000

# The default MongoDB field that will serve as the unique key for the
# target system.
DEFAULT_UNIQUE_KEY = "_id"

# Default host and facility for logging to the syslog.
DEFAULT_SYSLOG_HOST = "localhost:514"
DEFAULT_SYSLOG_FACILITY = "user"

# ROTATING LOGFILE
# The type of interval
# (seconds, minutes, hours... c.f. logging.handlers.TimedRotatingFileHandler)
DEFAULT_LOGFILE_WHEN = "midnight"
# The rollover interval
DEFAULT_LOGFILE_INTERVAL = 1
# Number of log files to keep
DEFAULT_LOGFILE_BACKUPCOUNT = 7
# The log format
DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"

# If a single meta collection is used, defines the default collection name
DEFAULT_META_COLLECTION_NAME = "__oplog"
# If a single meta collection is used, defines the default cap size
DEFAULT_META_COLLECTION_CAP_SIZE = 5 * 1024 * 1024
