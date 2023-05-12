from contextlib import contextmanager
from threading import Lock


class RWLock(object):
    _w_lock = Lock()
    _num_r_lock = Lock()
    _num_r = 0

    # Acquire the lock for reading
    def __r_acquire(self):
        self._num_r_lock.acquire()
        self._num_r += 1

        # Acquire write lock if first reader
        if self._num_r == 1:
            self._w_lock.acquire()

        self._num_r_lock.release()

    # Release the lock for reading
    def __r_release(self):
        self._num_r_lock.acquire()
        self._num_r -= 1

        # Release write lock if no more readers
        if self._num_r == 0:
            self._w_lock.release()

        self._num_r_lock.release()

    # Get read lock
    @contextmanager
    def r_locked(self):
        try:
            self.__r_acquire()
            yield
        finally:
            self.__r_release()

    # Acquire the lock for writing
    def __w_acquire(self):
        self._w_lock.acquire()

    # Release the lock for writing
    def __w_release(self):
        self._w_lock.release()

    # Get write lock
    @contextmanager
    def w_locked(self):
        try:
            self.__w_acquire()
            yield
        finally:
            self.__w_release()
