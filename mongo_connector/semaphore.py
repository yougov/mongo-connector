import threading


class Semaphore():
    def __init__(self, mutex=False):
        self.mutex = mutex
        self.waiting = 0
        self.lock = threading.Lock()

    def __enter__(self):
        if self.mutex:
            self.__acquire_lock()
        return self

    def __exit__(self, type, value, traceback):
        if self.mutex:
            self.__release_lock()

    def acquire(self):
        self.__acquire_lock()
        self.waiting += 1
        self.__release_lock()

    def release(self):
        self.__acquire_lock()
        self.waiting -= 1
        self.__release_lock()

    def proceed(self):
        if self.mutex:
            return not self.lock.locked()
        else:
            return self.waiting == 0

    def __acquire_lock(self):
        self.lock.acquire()

    def __release_lock(self):
        self.lock.release()
