import os
import sys
import json
import shutil
import logging
from mongo_connector import util
from mongo_connector.oplog_progress_stores.progress_store_base import ProgressStoreBase

LOG = logging.getLogger(__name__)


class FileProgressStore(ProgressStoreBase):

    def __init__(self, oplog_progress, oplog_checkpoint):
        self.oplog_progress = oplog_progress
        self.oplog_checkpoint = oplog_checkpoint

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = ("MongoConnector: Can't find %s, "
                            "attempting to create an empty progress log" %
                            self.oplog_checkpoint)
                LOG.warning(info_str)
                try:
                    # Create oplog progress file
                    open(self.oplog_checkpoint, "w").close()
                except IOError as e:
                    LOG.critical("MongoConnector: Could not "
                                 "create a progress log: %s" %
                                 str(e))
                    sys.exit(2)
            else:
                if (not os.access(self.oplog_checkpoint, os.W_OK)
                        and not os.access(self.oplog_checkpoint, os.R_OK)):
                    LOG.critical("Invalid permissions on %s! Exiting" %
                                 self.oplog_checkpoint)
                    sys.exit(2)

    def write_oplog_progress(self):
        """ Writes oplog progress to file provided by user
        """

        if self.oplog_checkpoint is None:
            return None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
        items = [[name, util.bson_ts_to_long(oplog_dict[name])]
                 for name in oplog_dict]
        if not items:
            return

        # write to temp file
        backup_file = self.oplog_checkpoint + '.backup'
        os.rename(self.oplog_checkpoint, backup_file)

        # for each of the threads write to file
        with open(self.oplog_checkpoint, 'w') as dest:
            if len(items) == 1:
                # Write 1-dimensional array, as in previous versions.
                json_str = json.dumps(items[0])
            else:
                # Write a 2d array to support sharded clusters.
                json_str = json.dumps(items)
            try:
                dest.write(json_str)
            except IOError:
                # Basically wipe the file, copy from backup
                dest.truncate()
                with open(backup_file, 'r') as backup:
                    shutil.copyfile(backup, dest)

        os.remove(backup_file)

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user.
        This method is only called once before any threads are spanwed.
        """

        if self.oplog_checkpoint is None:
            return None

        # Check for empty file
        try:
            if os.stat(self.oplog_checkpoint).st_size == 0:
                LOG.info("MongoConnector: Empty oplog progress file.")
                return None
        except OSError:
            return None

        with open(self.oplog_checkpoint, 'r') as progress_file:
            try:
                data = json.load(progress_file)
            except ValueError:
                LOG.exception(
                    'Cannot read oplog progress file "%s". '
                    'It may be corrupt after Mongo Connector was shut down'
                    'uncleanly. You can try to recover from a backup file '
                    '(may be called "%s.backup") or create a new progress file '
                    'starting at the current moment in time by running '
                    'mongo-connector --no-dump <other options>. '
                    'You may also be trying to read an oplog progress file '
                    'created with the old format for sharded clusters. '
                    'See https://github.com/10gen-labs/mongo-connector/wiki'
                    '/Oplog-Progress-File for complete documentation.'
                    % (self.oplog_checkpoint, self.oplog_checkpoint))
                return
            # data format:
            # [name, timestamp] = replica set
            # [[name, timestamp], [name, timestamp], ...] = sharded cluster
            if not isinstance(data[0], list):
                data = [data]
            with self.oplog_progress:
                self.oplog_progress.dict = dict(
                    (name, util.long_to_bson_ts(timestamp))
                    for name, timestamp in data)
