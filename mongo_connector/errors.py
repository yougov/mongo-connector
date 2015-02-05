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

"""Exceptions raised by the mongo_connector package."""


class MongoConnectorError(Exception):
    """Base class for all exceptions in the mongo_connector package
    """


class ConnectionFailed(MongoConnectorError):
    """Raised when mongo-connector can't connect to target system
    """


class OperationFailed(MongoConnectorError):
    """Raised for failed commands on the destination database
    """


class InvalidConfiguration(MongoConnectorError):
    """Raised when the user specifies an invalid configuration
    """


class EmptyDocsError(MongoConnectorError):
    """Raised on attempts to upsert empty sequences of documents
    """


class UpdateDoesNotApply(OperationFailed):
    """Raised when an update operation cannot be applied to a document."""
