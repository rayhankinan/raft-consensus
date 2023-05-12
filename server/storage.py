import pickle
from meta import ThreadSafeSingletonMeta
from log import Log
from config import ServerConfig


class Storage(metaclass=ThreadSafeSingletonMeta):
    _base_dir: str = "/mnt/data"

    def get_logs(self) -> list[Log]:
        try:
            with open(f"{self._base_dir}/logs", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def save_logs(self, logs: list[Log]) -> None:
        with open(f"{self._base_dir}/logs", "wb") as f:
            pickle.dump(logs, f)

    def get_current_term(self) -> int:
        try:
            with open(f"{self._base_dir}/current_term", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return 0

    def save_current_term(self, current_term: int) -> None:
        with open(f"{self._base_dir}/current_term", "wb") as f:
            pickle.dump(current_term, f)

    def get_voted_for(self) -> str:
        try:
            with open(f"{self._base_dir}/voted_for", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return ServerConfig().get("SERVER_HOSTNAME")

    def save_voted_for(self, voted_for: str) -> None:
        with open(f"{self._base_dir}/voted_for", "wb") as f:
            pickle.dump(voted_for, f)
