import threading


class LockingDict():

    def __init__(self):

        self.dict = {}
        self.lock = threading.Lock()

    def get_dict(self):
        return self.dict

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()
