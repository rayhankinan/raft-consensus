import pickle
from meta import ThreadSafeSingletonMeta
from log import Log
from config import ServerConfig
from rwlock import RWLock


class Storage(metaclass=ThreadSafeSingletonMeta):  # TODO: Tambahkan read-write lock
    _base_dir: str = "/mnt/data"
    _config: ServerConfig = ServerConfig()
    _rw_locks: dict[str, RWLock] = {
        "logs": RWLock(),
        "current_term": RWLock(),
        "voted_for": RWLock()
    }

    def get_logs(self) -> list[Log]:
        try:
            with self._rw_locks["logs"].r_locked(), open(f"{self._base_dir}/logs", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def save_logs(self, logs: list[Log]) -> None:
        with self._rw_locks["logs"].w_locked(), open(f"{self._base_dir}/logs", "wb") as f:
            pickle.dump(logs, f)

    def get_current_term(self) -> int:
        try:
            with self._rw_locks["current_term"].r_locked(), open(f"{self._base_dir}/current_term", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return 0

    def save_current_term(self, current_term: int) -> None:
        with self._rw_locks["current_term"].w_locked(), open(f"{self._base_dir}/current_term", "wb") as f:
            pickle.dump(current_term, f)

    def get_voted_for(self) -> str:
        try:
            with self._rw_locks["voted_for"].r_locked(), open(f"{self._base_dir}/voted_for", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return self._config.get("SERVER_HOSTNAME")

    def save_voted_for(self, voted_for: str) -> None:
        with self._rw_locks["voted_for"].w_locked(), open(f"{self._base_dir}/voted_for", "wb") as f:
            pickle.dump(voted_for, f)
