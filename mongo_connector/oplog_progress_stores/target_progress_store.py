
from mongo_connector import util
from mongo_connector.oplog_progress_stores.progress_store_base import ProgressStoreBase


class TargetProgressStore(ProgressStoreBase):

    def __init__(self, oplog_progress, doc_manager, oplog_collection):
        self.oplog_progress = oplog_progress
        self.doc_manager = doc_manager
        self.oplog_collection = oplog_collection

    def read_oplog_progress(self):
        data = self.doc_manager.read_oplog_progress(self.oplog_collection)

        if len(data) == 0:
            return None

        with self.oplog_progress:
            self.oplog_progress.dict = dict(
                (item["name"], util.long_to_bson_ts(item["checkpoint"]))
                for item in data)

    def write_oplog_progress(self):
        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
        items = [{
            "name": name,
            "checkpoint": util.bson_ts_to_long(oplog_dict[name])
            } for name in oplog_dict]
        if not items:
            return

        self.doc_manager.write_oplog_progress(self.oplog_collection, items)
