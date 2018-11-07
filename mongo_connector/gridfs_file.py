import gridfs

from mongo_connector import errors, util

wrap_exceptions = util.exception_wrapper(
    {gridfs.errors.CorruptGridFile: errors.OperationFailed}
)


class GridFSFile(object):
    @wrap_exceptions
    def __init__(self, collection, doc):
        self._id = doc["_id"]
        self.f = gridfs.GridOut(collection, file_document=doc)
        self.filename = self.f.filename
        self.length = self.f.length
        self.upload_date = self.f.upload_date
        self.md5 = self.f.md5

    def get_metadata(self):
        result = {"_id": self._id, "upload_date": self.upload_date, "md5": self.md5}
        if self.filename is not None:
            result["filename"] = self.filename
        return result

    def __len__(self):
        return self.length

    @wrap_exceptions
    def read(self, n=-1):
        return self.f.read(n)
