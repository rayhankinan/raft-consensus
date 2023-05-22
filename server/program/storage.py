import pickle
import os
from threading import Lock
from data import MembershipLog, StateLog, Address
from . import ServerConfig


class StorageMeta(type):  # Thread Safe Singleton
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class Storage(metaclass=StorageMeta):  # Ini Singleton
    __base_dir: str = "/mnt/data"
    __config: ServerConfig = ServerConfig()

    def get_membership_log(self) -> list[MembershipLog]:
        try:
            with open(f"{self.__base_dir}/membership_log.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def save_membership_log(self, logs: list[MembershipLog]) -> None:
        os.makedirs(self.__base_dir, exist_ok=True)

        with open(f"{self.__base_dir}/membership_log.pickle", "wb") as f:
            pickle.dump(logs, f)

    def get_state_log(self) -> list[StateLog]:
        try:
            with open(f"{self.__base_dir}/state_log.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def save_state_log(self, logs: list[StateLog]) -> None:
        os.makedirs(self.__base_dir, exist_ok=True)

        with open(f"{self.__base_dir}/state_log.pickle", "wb") as f:
            pickle.dump(logs, f)

    def get_current_term(self) -> int:
        try:
            with open(f"{self.__base_dir}/current_term.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return 0

    def save_current_term(self, current_term: int) -> None:
        os.makedirs(self.__base_dir, exist_ok=True)

        with open(f"{self.__base_dir}/current_term.pickle", "wb") as f:
            pickle.dump(current_term, f)

    def get_voted_for(self) -> Address:
        try:
            with open(f"{self.__base_dir}/voted_for.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return self.__config.get("SERVER_ADDRESS")

    def save_voted_for(self, voted_for: str) -> None:
        os.makedirs(self.__base_dir, exist_ok=True)

        with open(f"{self.__base_dir}/voted_for.pickle", "wb") as f:
            pickle.dump(voted_for, f)

    def get_current_leader_address(self) -> Address:
        try:
            with open(f"{self.__base_dir}/current_leader_address.pickle", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return self.__config.get("LEADER_ADDRESS")

    def save_current_leader_address(self, current_leader_address: Address) -> None:
        os.makedirs(self.__base_dir, exist_ok=True)

        with open(f"{self.__base_dir}/current_leader_address.pickle", "wb") as f:
            pickle.dump(current_leader_address, f)
