import threading


class Semaphore():
    def __init__(self):
        self.waiting = 0
        self.lock = threading.Lock()

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, type, value, traceback):
        self.release_lock()

    def wait(self):
        self.waiting += 1

    def notify(self):
        self.waiting -= 1

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()
