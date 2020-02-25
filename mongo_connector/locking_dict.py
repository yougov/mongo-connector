import threading


class LockingDict:
    def __init__(self):

        self.dict = {}
        self.lock = threading.Lock()

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, type, value, traceback):
        self.release_lock()

    def get_dict(self):
        return self.dict

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()
