import time
import copy
from threading import Lock
from sched import scheduler
from queue import Queue
from . import MembershipLog, StateLog, Role, Address, Storage, ServerConfig, ServerInfo, RWLock


class RaftNodeMeta(type):
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class RaftNode(metaclass=RaftNodeMeta):
    # Utility
    __scheduler: scheduler = scheduler(time.time, time.sleep)
    __storage: Storage = Storage()
    __config: ServerConfig = ServerConfig()
    __rw_locks: dict[str, RWLock] = {
        "membership_log": RWLock(),
        "state_log": RWLock(),
        "current_term": RWLock(),
        "voted_for": RWLock(),
        "state_machine": RWLock(),
        "state_commit_index": RWLock(),
        "state_last_applied": RWLock(),
        "current_known_address": RWLock(),
        "known_address_commit_index": RWLock(),
        "known_address_last_applied": RWLock(),
        "current_role": RWLock(),
        "current_leader_address": RWLock(),
    }

    # Persistent state on all servers
    __membership_log: list[MembershipLog] = __storage.get_membership_log()
    __state_log: list[StateLog] = __storage.get_state_log()
    __current_term: int = __storage.get_current_term()
    __voted_for: Address = __storage.get_voted_for()

    # Volatile queue state on all servers
    __state_machine: Queue[str] = Queue()
    __state_commit_index: int = 0
    __state_last_applied: int = 0

    # Volatile address state on all servers
    __current_known_address: dict[Address, ServerInfo] = {}
    __known_address_commit_index = 0
    __known_address_last_applied = 0

    # Other state
    __current_role: Role = Role.FOLLOWER
    __current_leader_address: Address = __config.get("LEADER_ADDRESS")

    # Public Method (Read)
    def get_current_term(self) -> int:
        with self.__rw_locks["current_term"].r_locked():
            return self.__current_term

    # Public Method (Read)
    def get_current_known_address(self) -> dict[Address, ServerInfo]:
        with self.__rw_locks["current_known_address"].r_locked():
            return self.__current_known_address

    # Public Method (Read)
    def get_current_leader_address(self) -> Address:
        with self.__rw_locks["current_leader_address"].r_locked():
            return self.__current_leader_address

    # Public Method (Read)
    def get_current_role(self) -> Role:
        with self.__rw_locks["current_role"].r_locked():
            return self.__current_role

    # Public Method (Read): Test untuk client
    def get_membership_log(self) -> list[MembershipLog]:
        with self.__rw_locks["membership_log"].r_locked():
            return self.__membership_log

    # Public Method (Write)
    def leader_startup(self) -> None:
        with self.__rw_locks["membership_log"].r_locked(),  self.__rw_locks["current_role"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
            snapshot_current_state = copy.deepcopy(
                self.__current_role
            )
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                self.__current_role = Role.LEADER
                address = self.__config.get("SERVER_ADDRESS")
                server_info = ServerInfo(
                    len(self.__membership_log),
                    0,
                )

                self.__current_known_address.clear()
                self.__current_known_address[address] = server_info
            except:
                self.__current_role = snapshot_current_state
                self.__current_known_address = snapshot_current_known_address
                raise RuntimeError("Failed to initialize")

    # Public Method (Write)
    def add_membership_log(self, membership_log: MembershipLog) -> None:
        with self.__rw_locks["membership_log"].w_locked():
            snapshot_membership_log = copy.deepcopy(self.__membership_log)

            try:
                self.__membership_log.append(membership_log)
            except:
                self.__membership_log = snapshot_membership_log
                raise RuntimeError("Failed to add membership log")

    # Public Method (Write)
    def commit_membership_log(self) -> None:
        with self.__rw_locks["membership_log"].r_locked(), self.__rw_locks["known_address_commit_index"].w_locked(), self.__rw_locks["known_address_last_applied"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
            snapshot_known_address_commit_index = copy.deepcopy(
                self.__known_address_commit_index
            )
            snapshot_known_address_last_applied = copy.deepcopy(
                self.__known_address_last_applied
            )
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                self.__storage.save_membership_log(self.__membership_log)
                self.__known_address_commit_index = len(self.__membership_log)

                while self.__known_address_last_applied < self.__known_address_commit_index:
                    last_applied_membership_log = self.__membership_log[
                        self.__known_address_last_applied
                    ]

                    match last_applied_membership_log.command:
                        case "ADD_NODE":
                            entries = {
                                address: ServerInfo(
                                    len(self.__membership_log),
                                    0,
                                ) for address in last_applied_membership_log.args
                            }

                            self.__current_known_address.update(entries)
                        case "REMOVE_NODE":
                            for address in last_applied_membership_log.args:
                                self.__current_known_address.pop(address, None)
                        case _:
                            raise RuntimeError("Invalid log command")

                    self.__known_address_last_applied += 1
            except:
                self.__known_address_commit_index = snapshot_known_address_commit_index
                self.__known_address_last_applied = snapshot_known_address_last_applied
                self.__current_known_address = snapshot_current_known_address

                self.__storage.save_membership_log(self.__membership_log)
                raise RuntimeError("Failed to commit log")
