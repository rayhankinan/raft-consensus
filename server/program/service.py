import rpyc
import asyncio
import time
import copy
from threading import Lock
from sched import scheduler
from queue import Queue
from . import Log, State, Address, Storage, ServerConfig, RWLock, dynamically_call_procedure, serialize, deserialize


class RaftNodeMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RaftNode(metaclass=RaftNodeMeta):
    # Utility
    __scheduler: scheduler = scheduler(time.time, time.sleep)
    __storage: Storage = Storage()
    __config: ServerConfig = ServerConfig()
    __rw_locks: dict[str, RWLock] = {
        "logs": RWLock(),
        "current_term": RWLock(),
        "voted_for": RWLock(),
        "state_machine": RWLock(),
        "commit_index": RWLock(),
        "last_applied": RWLock(),
        "current_state": RWLock(),
        "current_known_address": RWLock(),
        "current_leader_address": RWLock(),
    }

    # Persistent state on all servers
    __logs: list[Log] = __storage.get_logs()
    __current_term: int = __storage.get_current_term()
    __voted_for: Address = __storage.get_voted_for()

    # Volatile state on all servers
    __state_machine: Queue[str] = Queue()
    __commit_index: int = 0
    __last_applied: int = 0
    __current_state: State = State.FOLLOWER
    __current_known_address: set[Address] = set()
    __current_leader_address: Address = __config.get("LEADER_ADDRESS")

    # Private Method
    def __apply_log(self, log: Log) -> None:
        match log.command:
            case "ENQUEUE":
                raw_value, = log.args
                value = str(raw_value)

                with self.__rw_locks["state_machine"].w_locked():
                    snapshot_state_machine = copy.deepcopy(
                        self.__state_machine
                    )

                    try:
                        self.__state_machine.put(value)
                    except:
                        self.__state_machine = snapshot_state_machine
                        raise RuntimeError("Queue is full")

            case "DEQUEUE":
                with self.__rw_locks["state_machine"].w_locked():
                    snapshot_state_machine = copy.deepcopy(
                        self.__state_machine
                    )

                    try:
                        self.__state_machine.get()
                    except:
                        self.__state_machine = snapshot_state_machine
                        raise RuntimeError("Queue is empty")

            case "ADD_NODE":
                raw_hostname, raw_port = log.args
                hostname, port = str(raw_hostname), int(raw_port)

                with self.__rw_locks["current_known_address"].w_locked():
                    snapshot_current_known_address = copy.deepcopy(
                        self.__current_known_address
                    )

                    try:
                        self.__current_known_address.add(
                            Address(hostname, port)
                        )
                    except:
                        self.__current_known_address = snapshot_current_known_address
                        raise RuntimeError("Node already exists")

            case "REMOVE_NODE":
                raw_hostname, raw_port = log.args
                hostname, port = str(raw_hostname), int(raw_port)

                with self.__rw_locks["current_known_address"].w_locked():
                    snapshot_current_known_address = copy.deepcopy(
                        self.__current_known_address
                    )

                    try:
                        self.__current_known_address.remove(
                            Address(hostname, port)
                        )
                    except:
                        self.__current_known_address = snapshot_current_known_address
                        raise RuntimeError("Node not found")

            case _:
                raise RuntimeError("Invalid log command")

    # Public Method
    def get_current_term(self) -> int:
        with self.__rw_locks["current_term"].r_locked():
            return self.__current_term

    # Public Method: Test untuk client
    def get_current_known_address(self) -> set[Address]:
        with self.__rw_locks["current_known_address"].r_locked():
            return self.__current_known_address

    # Public Method: Test untuk client
    def get_logs(self) -> list[Log]:
        with self.__rw_locks["logs"].r_locked():
            return self.__logs

    # Public Method
    def initialize(self) -> None:
        with self.__rw_locks["current_state"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
            snapshot_current_state = copy.deepcopy(self.__current_state)
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                if self.__current_leader_address == self.__config.get("SERVER_ADDRESS"):
                    self.__current_state = State.LEADER
                    self.__current_known_address.add(
                        self.__config.get("SERVER_ADDRESS")
                    )

                else:
                    hostname, port = self.__current_leader_address
                    conn: rpyc.Connection = rpyc.connect(
                        hostname,
                        port,
                        service=ServerService,
                    )

                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "apply_membership",
                        )
                    )
            except:
                self.__current_state = snapshot_current_state
                self.__current_known_address = snapshot_current_known_address
                raise RuntimeError("Failed to initialize")

    # Public Method
    def add_log(self, log: Log) -> None:
        with self.__rw_locks["logs"].w_locked():
            snapshot_logs = copy.deepcopy(self.__logs)

            try:
                self.__logs.append(log)
            except:
                self.__logs = snapshot_logs
                raise RuntimeError("Failed to add log")

    # Public Method
    def commit_log(self) -> None:
        with self.__rw_locks["logs"].r_locked(), self.__rw_locks["commit_index"].w_locked(), self.__rw_locks["last_applied"].w_locked():
            snapshot_copy_index = copy.deepcopy(self.__commit_index)
            snapshot_last_applied = copy.deepcopy(self.__last_applied)

            try:
                # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                self.__storage.save_logs(self.__logs)
                self.__commit_index = len(self.__logs) - 1

                while self.__last_applied < self.__commit_index:
                    last_applied_log = self.__logs[self.__last_applied]
                    self.__apply_log(last_applied_log)
                    self.__last_applied += 1
            except:
                self.__commit_index = snapshot_copy_index
                self.__last_applied = snapshot_last_applied

                raise RuntimeError("Failed to commit log")


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __node: RaftNode
    __config: ServerConfig
    __conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__node = RaftNode()
        self.__config = ServerConfig()
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    @rpyc.exposed
    def apply_membership(self) -> None:
        follower_address: Address = deserialize(
            asyncio.run(
                dynamically_call_procedure(
                    self.__conn,
                    "get_current_address",
                )
            )
        )

        new_log = Log(
            self.__node.get_current_term(),
            "ADD_NODE",
            follower_address,
        )

        self.__node.add_log(new_log)
        # TODO: Broadcast add log to all nodes and wait for majority

        self.__node.commit_log()
        # TODO: Broadcast commit and apply log to all nodes and wait for majority

    @rpyc.exposed
    def get_current_address(self) -> bytes:
        return serialize(self.__config.get("SERVER_ADDRESS"))

    # Test untuk client
    @rpyc.exposed
    def print_logs(self) -> None:
        print(self.__node.get_logs())

    # Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        print(self.__node.get_current_known_address())
