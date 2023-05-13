import pickle
import os
from threading import Lock
from . import Log, ServerConfig, RWLock, Address


class StorageMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Storage(metaclass=StorageMeta):
    _base_dir: str = "/mnt/data"
    _config: ServerConfig = ServerConfig()
    _rw_locks: dict[str, RWLock] = {
        "logs": RWLock(),
        "current_term": RWLock(),
        "voted_for": RWLock()
    }

    def get_logs(self) -> list[Log]:
        try:
            with self._rw_locks["logs"].r_locked(), open(f"{self._base_dir}/logs.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def save_logs(self, logs: list[Log]) -> None:
        os.makedirs(self._base_dir, exist_ok=True)

        with self._rw_locks["logs"].w_locked(), open(f"{self._base_dir}/logs.pickle", "wb") as f:
            pickle.dump(logs, f)

    def get_current_term(self) -> int:
        try:
            with self._rw_locks["current_term"].r_locked(), open(f"{self._base_dir}/current_term.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return 0

    def save_current_term(self, current_term: int) -> None:
        os.makedirs(self._base_dir, exist_ok=True)

        with self._rw_locks["current_term"].w_locked(), open(f"{self._base_dir}/current_term.pickle", "wb") as f:
            pickle.dump(current_term, f)

    def get_voted_for(self) -> Address:
        try:
            with self._rw_locks["voted_for"].r_locked(), open(f"{self._base_dir}/voted_for.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return self._config.get("SERVER_ADDRESS")

    def save_voted_for(self, voted_for: str) -> None:
        os.makedirs(self._base_dir, exist_ok=True)

        with self._rw_locks["voted_for"].w_locked(), open(f"{self._base_dir}/voted_for.pickle", "wb") as f:
            pickle.dump(voted_for, f)
