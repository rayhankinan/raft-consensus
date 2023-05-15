from contextlib import contextmanager
from threading import Lock


class RWLock(object):
    __w_lock: Lock
    __num_r_lock: Lock
    __num_r: int

    def __init__(self) -> None:
        self.__w_lock = Lock()
        self.__num_r_lock = Lock()
        self.__num_r = 0

    # Acquire the lock for reading
    def __r_acquire(self):
        self.__num_r_lock.acquire()
        self.__num_r += 1

        # Acquire write lock if first reader
        if self.__num_r == 1:
            self.__w_lock.acquire()

        self.__num_r_lock.release()

    # Release the lock for reading
    def __r_release(self):
        self.__num_r_lock.acquire()
        self.__num_r -= 1

        # Release write lock if no more readers
        if self.__num_r == 0:
            self.__w_lock.release()

        self.__num_r_lock.release()

    # Get read lock
    @contextmanager
    def r_locked(self):
        self.__r_acquire()

        try:
            yield
        finally:
            self.__r_release()

    # Acquire the lock for writing
    def __w_acquire(self):
        self.__w_lock.acquire()

    # Release the lock for writing
    def __w_release(self):
        self.__w_lock.release()

    # TODO: Implementasikan
    # Upgrade read lock to write lock
    @contextmanager
    def r_to_w_locked(self):
        self.__r_release()
        self.__w_acquire()

        try:
            yield
        finally:
            self.__w_release()

    # Get write lock
    # @contextmanager
    # def w_locked(self):
    #     self.__w_acquire()

    #     try:
    #         yield
    #     finally:
    #         self.__w_release()
