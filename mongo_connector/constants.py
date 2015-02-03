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

# Maximum # of documents to process before recording timestamp
# default = -1 (no maximum)
DEFAULT_BATCH_SIZE = -1
# Interval in seconds between doc manager flushes (i.e. auto commit)
# default = None (never auto commit)
DEFAULT_COMMIT_INTERVAL = None
# Maximum # of documents to send in a single bulk request through a
# DocManager. This only affects DocManagers that cannot stream their
# requests.
DEFAULT_MAX_BULK = 500
# ROTATING LOGFILE
# The type of interval (seconds, minutes, hours... cf TimedRotatingFileHandler class)
DEFAULT_LOGFILE_WHEN = "midnight"
# The rollover interval
DEFAULT_LOGFILE_INTERVAL = 1
# Number of log file to keep
DEFAULT_LOGFILE_BACKUPCOUNT = 7
