import threading


class LockingDict():

    def __init__(self):

        self.dict = {}
        self.lock = threading.Lock()

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()
