
class ProgressStoreBase(object):

    def write_oplog_progress(self):
        raise NotImplementedError

    def read_oplog_progress(self):
        raise NotImplementedError
