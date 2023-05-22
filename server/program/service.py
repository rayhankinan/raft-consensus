import time
import copy
import asyncio
import rpyc
import threading
import random
from threading import Lock
from queue import Queue
from typing import Any, Coroutine, Literal, Tuple, Optional
from data import Address, ServerInfo, MembershipLog, StateLog, Role
from . import Storage, ServerConfig, RWLock, dynamically_call_procedure, wait_for_majority, wait_for_all, serialize, deserialize


def create_connection(address: Address) -> Optional[rpyc.Connection]:
    hostname, port = address
    conn: rpyc.Connection

    try:
        conn = rpyc.connect(
            hostname,
            port,
            service=ServerService,
        )
        return conn

    except:
        return None


class RaftNodeMeta(type):  # Thread Safe Singleton
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class RaftNode(metaclass=RaftNodeMeta):  # Ini Singleton
    # Utility
    __storage: Storage = Storage()
    __config: ServerConfig = ServerConfig()
    __rw_locks: dict[str, RWLock] = {
        "membership_log": RWLock(),
        "state_log": RWLock(),
        "current_term": RWLock(),
        "voted_for": RWLock(),
        "current_leader_address": RWLock(),
        "state_machine": RWLock(),
        "state_commit_index": RWLock(),
        "state_last_applied": RWLock(),
        "current_known_address": RWLock(),
        "known_address_commit_index": RWLock(),
        "known_address_last_applied": RWLock(),
        "current_role": RWLock(),
    }

    # Persistent state on all servers
    __membership_log: list[MembershipLog] = __storage.get_membership_log()
    __state_log: list[StateLog] = __storage.get_state_log()
    __current_term: int = __storage.get_current_term()
    __voted_for: Address = __storage.get_voted_for()
    __current_leader_address: Address = __storage.get_current_leader_address()

    # Volatile queue state on all servers
    __state_machine: Queue[str] = Queue()
    __state_commit_index: int = 0
    __state_last_applied: int = 0

    # Volatile address state on all servers
    __current_known_address: dict[Address, ServerInfo] = {}
    __known_address_commit_index = 0
    __known_address_last_applied = 0
    __current_role: Role = Role.FOLLOWER

    # Heartbeat: TODO Tambahkan lock
    __heartbeat_interval: float = 1.0
    __heartbeat_timeout: float = random.uniform(3.0, 4.0)
    __last_heartbeat_time = time.time()

    # Threads: TODO Tambahkan lock
    __heartbeat_thread: threading.Thread
    __timeout_thread: threading.Thread

    # Public Method (Read): Testing untuk client
    def get_current_known_address(self) -> dict[Address, ServerInfo]:
        with self.__rw_locks["current_known_address"].r_locked():
            return self.__current_known_address

    # Public Method (Read): Testing untuk client
    def get_membership_log(self) -> list[MembershipLog]:
        with self.__rw_locks["membership_log"].r_locked():
            return self.__membership_log

    # Public Method (Read): Testing untuk client
    def get_current_term(self) -> int:
        with self.__rw_locks["current_term"].r_locked():
            return self.__current_term

    # Public Method (Read): Testing untuk client
    def get_current_role(self) -> Role:
        with self.__rw_locks["current_role"].r_locked():
            return self.__current_role

    # Public Method (Read): Testing untuk client
    def get_known_address_commit_index(self) -> int:
        with self.__rw_locks["known_address_commit_index"].r_locked():
            return self.__known_address_commit_index

    # Public Method (Read): Testing untuk client
    def get_known_address_last_applied(self) -> int:
        with self.__rw_locks["known_address_last_applied"].r_locked():
            return self.__known_address_last_applied

    # Public Method (Read): Testing untuk client
    def get_leader_address(self) -> Address:
        with self.__rw_locks["current_leader_address"].r_locked():
            return self.__current_leader_address

    # Public Method (Read): Testing untuk client
    def get_state_machine(self) -> Queue[str]:
        with self.__rw_locks["state_machine"].r_locked():
            return self.__state_machine.queue

    # Public Method (Read): Testing untuk client
    def get_state_commit_index(self) -> int:
        with self.__rw_locks["state_commit_index"].r_locked():
            return self.__state_commit_index

    # Public Method (Read): Testing untuk client
    def get_state_last_applied(self) -> int:
        with self.__rw_locks["state_last_applied"].r_locked():
            return self.__state_last_applied

    # Public Method (Read): Testing untuk client
    def get_state_log(self) -> list[StateLog]:
        with self.__rw_locks["state_log"].r_locked():
            return self.__state_log

    # Public Method (Write)
    def start(self) -> None:
        with self.__rw_locks["current_leader_address"].r_locked():
            if self.__current_leader_address != self.__config.get("SERVER_ADDRESS"):
                conn = create_connection(self.__current_leader_address)
                server_addresses = (
                    self.__config.get("SERVER_ADDRESS"),
                )

                if conn == None:
                    raise RuntimeError("Leader is down")

                asyncio.run(
                    dynamically_call_procedure(
                        conn,
                        "add_server",
                        serialize(server_addresses),
                    )
                )

            else:
                with self.__rw_locks["current_role"].w_locked():
                    snapshot_current_role = copy.deepcopy(
                        self.__current_role
                    )

                    try:
                        self.__current_role = Role.LEADER

                        with self.__rw_locks["membership_log"].w_locked():
                            snapshot_membership_log = copy.deepcopy(
                                self.__membership_log
                            )

                            try:
                                new_membership_log = MembershipLog(
                                    self.__current_term,
                                    "ADD_NODE",
                                    (self.__config.get("SERVER_ADDRESS"),)
                                )
                                print(
                                    f"Adding default leader {self.__config.get('SERVER_ADDRESS')} to membership log")
                                self.__membership_log.append(
                                    new_membership_log)

                                with self.__rw_locks["known_address_commit_index"].w_locked():
                                    snapshot_known_address_commit_index = copy.deepcopy(
                                        self.__known_address_commit_index
                                    )

                                    try:
                                        self.__known_address_commit_index = len(
                                            self.__membership_log
                                        )

                                        with self.__rw_locks["known_address_last_applied"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
                                            snapshot_known_address_last_applied = copy.deepcopy(
                                                self.__known_address_last_applied
                                            )
                                            snapshot_current_known_address = copy.deepcopy(
                                                self.__current_known_address
                                            )

                                        try:
                                            current_address = self.__config.get(
                                                "SERVER_ADDRESS"
                                            )
                                            current_server_info = ServerInfo(
                                                len(self.__membership_log) - 1,
                                                0,
                                                len(self.__state_log),
                                                -1,
                                            )
                                            entries = {
                                                current_address: current_server_info
                                            }

                                            self.__current_known_address.update(
                                                entries
                                            )
                                            self.__known_address_last_applied += 1
                                        except:
                                            self.__known_address_last_applied = snapshot_known_address_last_applied
                                            self.__current_known_address = snapshot_current_known_address
                                            raise RuntimeError(
                                                "Failed to update current known address"
                                            )
                                    except:
                                        self.__known_address_commit_index = snapshot_known_address_commit_index
                                        raise RuntimeError(
                                            "Failed to update known address"
                                        )

                            except:
                                self.__membership_log = snapshot_membership_log
                                raise RuntimeError("Failed to add log")

                    except:
                        self.__current_role = snapshot_current_role
                        raise RuntimeError("Failed to initialize")

                # heartbeat
                self.__heartbeat_thread = threading.Thread(
                    target=self.start_heartbeat)
                self.__heartbeat_thread.daemon = True
                self.__heartbeat_thread.start()

            self.__last_heartbeat_time = time.time()
            self.__heartbeat_timeout = random.uniform(2.0, 3.0)
            self.__timeout_thread = threading.Thread(target=self.check_timeout)
            self.__timeout_thread.daemon = True
            self.__timeout_thread.start()

    # Public Method (Write)
    def add_server(self, follower_addresses: Tuple[Address, ...]) -> None:
        with self.__rw_locks["current_role"].r_locked():
            if self.__current_role != Role.LEADER:
                with self.__rw_locks["current_leader_address"].r_locked():
                    conn = create_connection(
                        self.__current_leader_address
                    )

                    if conn == None:
                        raise RuntimeError("Leader is down")

                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "add_server",
                            serialize(follower_addresses),
                        )
                    )
                    return

            with self.__rw_locks["current_term"].r_locked():
                new_membership_log = MembershipLog(
                    self.__current_term,
                    "ADD_NODE",
                    follower_addresses,
                )

                with self.__rw_locks["membership_log"].w_locked():
                    snapshot_membership_log = copy.deepcopy(
                        self.__membership_log
                    )

                    try:
                        # Append in Leader
                        self.__membership_log.append(
                            new_membership_log
                        )

                        # Append in Current Follower
                        # Broadcast append_membership_logs to all nodes and wait for majority
                        with self.__rw_locks["current_known_address"].r_locked(), self.__rw_locks["known_address_commit_index"].r_locked():
                            known_follower_addresses = {
                                address: server_info
                                for address, server_info in self.__current_known_address.items()
                                if address != self.__config.get("SERVER_ADDRESS")
                            }

                            # Hanya broadcast jika ada Current Follower
                            if len(known_follower_addresses) > 0:
                                list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                ]

                                for address, server_info in known_follower_addresses.items():
                                    conn = create_connection(address)

                                    if conn != None:
                                        list_of_coroutine.append(
                                            dynamically_call_procedure(
                                                conn,
                                                "append_membership_logs",
                                                serialize(self.__current_term),
                                                serialize(
                                                    server_info.next_index - 1
                                                ),
                                                serialize(
                                                    self.__membership_log[server_info.next_index - 1].term
                                                ),
                                                serialize(
                                                    self.__membership_log[server_info.next_index:]
                                                ),
                                                serialize(
                                                    self.__known_address_commit_index
                                                ),
                                            )
                                        )

                                asyncio.run(
                                    wait_for_majority(
                                        *list_of_coroutine
                                    )
                                )

                            # Commit and Apply in Leader
                            # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                            self.__storage.save_membership_log(
                                self.__membership_log
                            )

                            with self.__rw_locks["known_address_commit_index"].r_to_w_locked():
                                snapshot_known_address_commit_index = copy.deepcopy(
                                    self.__known_address_commit_index
                                )

                                try:
                                    self.__known_address_commit_index = len(
                                        self.__membership_log
                                    )

                                    with self.__rw_locks["known_address_last_applied"].w_locked(), self.__rw_locks["current_known_address"].r_to_w_locked():
                                        snapshot_known_address_last_applied = copy.deepcopy(
                                            self.__known_address_last_applied
                                        )
                                        snapshot_current_known_address = copy.deepcopy(
                                            self.__current_known_address
                                        )

                                        try:
                                            while self.__known_address_last_applied < self.__known_address_commit_index:
                                                last_applied_membership_log = self.__membership_log[
                                                    self.__known_address_last_applied
                                                ]

                                                match last_applied_membership_log.command:
                                                    case "ADD_NODE":
                                                        entries = {
                                                            address: ServerInfo(
                                                                len(self.__membership_log) - 1,
                                                                0,
                                                                len(self.__state_log),
                                                                -1,
                                                            ) for address in last_applied_membership_log.args
                                                        }
                                                        self.__current_known_address.update(
                                                            entries
                                                        )
                                                    case "REMOVE_NODE":
                                                        for address in last_applied_membership_log.args:
                                                            self.__current_known_address.pop(
                                                                address,
                                                                None,
                                                            )
                                                    case _:
                                                        raise RuntimeError(
                                                            "Invalid log command")

                                                self.__known_address_last_applied += 1

                                            # Commit and Apply in Current Follower
                                            # Broadcast commit_membership_logs to all nodes and wait for majority
                                            new_known_follower_addresses = {
                                                address: server_info
                                                for address, server_info in self.__current_known_address.items()
                                                if address != self.__config.get("SERVER_ADDRESS")
                                            }

                                            # Hanya broadcast jika ada follower
                                            if len(new_known_follower_addresses) > 0:
                                                list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                                ]

                                                for address, server_info in new_known_follower_addresses.items():
                                                    conn = create_connection(
                                                        address
                                                    )

                                                    if conn != None:
                                                        list_of_coroutine.append(
                                                            dynamically_call_procedure(
                                                                conn,
                                                                "commit_membership_logs",
                                                            )
                                                        )

                                                asyncio.run(
                                                    wait_for_majority(
                                                        *list_of_coroutine
                                                    )
                                                )

                                            # Append in New Follower
                                            list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                            ]

                                            for follower_address in follower_addresses:
                                                conn = create_connection(
                                                    follower_address
                                                )

                                                if conn != None:
                                                    list_of_coroutine.append(
                                                        dynamically_call_procedure(
                                                            conn,
                                                            "append_membership_logs",
                                                            serialize(
                                                                self.__current_term),
                                                            serialize(-1),
                                                            serialize(
                                                                self.__current_term
                                                            ),
                                                            serialize(
                                                                self.__membership_log[:]
                                                            ),
                                                            serialize(
                                                                self.__known_address_commit_index
                                                            ),
                                                        )
                                                    )

                                            asyncio.run(
                                                wait_for_all(
                                                    *list_of_coroutine
                                                )
                                            )

                                            # Commit in New Follower
                                            list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                            ]

                                            for follower_address in follower_addresses:
                                                conn = create_connection(
                                                    follower_address)

                                                if conn != None:
                                                    list_of_coroutine.append(
                                                        dynamically_call_procedure(
                                                            conn,
                                                            "commit_membership_logs",
                                                        )
                                                    )

                                            asyncio.run(
                                                wait_for_all(
                                                    *list_of_coroutine
                                                )
                                            )

                                        except:
                                            self.__known_address_last_applied = snapshot_known_address_last_applied
                                            self.__current_known_address = snapshot_current_known_address
                                            raise RuntimeError(
                                                "Failed to update known address last applied"
                                            )
                                except:
                                    self.__known_address_commit_index = snapshot_known_address_commit_index
                                    raise RuntimeError(
                                        "Failed to update known address"
                                    )
                    except:
                        self.__membership_log = snapshot_membership_log
                        self.__storage.save_membership_log(
                            self.__membership_log
                        )
                        raise RuntimeError("Failed to add server")

    # Public Method (Write)
    def append_membership_logs(self, term: int, prev_log_index: int, prev_log_term: int, new_membership_logs: list[MembershipLog], leader_commit_index: int) -> None:
        # Append in Follower
        with self.__rw_locks["current_term"].r_locked():
            if term < self.__current_term:
                raise RuntimeError("Term is too old")

            with self.__rw_locks["membership_log"].r_locked():
                if prev_log_index > 0 and self.__membership_log[prev_log_index].term != prev_log_term:
                    # Kurangi nilai prev_log_index pada RPC yang dipanggil oleh leader dan ulangi lagi
                    # Dikarenakan byzantine leader, maka follower harus mengecek apakah prev_log_index yang dikirimkan oleh leader valid
                    raise RuntimeError("Prev Log Term does not match")

                temporary_length = len(self.__membership_log)
                temporary_index = prev_log_index

                with self.__rw_locks["membership_log"].r_to_w_locked():
                    snapshot_membership_log = copy.deepcopy(
                        self.__membership_log
                    )

                    try:
                        for membership_log in new_membership_logs:
                            temporary_index += 1

                            if temporary_index < temporary_length:
                                self.__membership_log[temporary_index] = membership_log
                            else:
                                self.__membership_log.append(membership_log)

                        final_length = len(self.__membership_log)

                        with self.__rw_locks["known_address_last_applied"].r_locked():
                            if leader_commit_index <= self.__known_address_commit_index:
                                return

                            with self.__rw_locks["known_address_commit_index"].r_to_w_locked():
                                snapshot_known_address_commit_index = copy.deepcopy(
                                    self.__known_address_commit_index
                                )

                                try:
                                    self.__known_address_commit_index = min(
                                        leader_commit_index,
                                        final_length,
                                    )
                                except:
                                    self.__known_address_commit_index = snapshot_known_address_commit_index
                                    raise RuntimeError(
                                        "Failed to update known address commit index"
                                    )
                    except:
                        self.__membership_log = snapshot_membership_log
                        raise RuntimeError("Failed to append membership logs")

    # Public Method (Write)
    def commit_membership_logs(self) -> None:
        # Commit in Follower
        with self.__rw_locks["membership_log"].w_locked():
            snapshot_membership_log = copy.deepcopy(
                self.__membership_log
            )

            try:
                # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                self.__storage.save_membership_log(
                    self.__membership_log
                )

                # Apply in Follower
                with self.__rw_locks["known_address_commit_index"].w_locked():
                    snapshot_known_address_commit_index = copy.deepcopy(
                        self.__known_address_commit_index
                    )

                    try:
                        self.__known_address_commit_index = len(
                            self.__membership_log
                        )

                        with self.__rw_locks["known_address_last_applied"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
                            snapshot_known_address_last_applied = copy.deepcopy(
                                self.__known_address_last_applied
                            )
                            snapshot_current_known_address = copy.deepcopy(
                                self.__current_known_address
                            )

                            try:
                                while self.__known_address_last_applied < self.__known_address_commit_index:
                                    last_applied_membership_log = self.__membership_log[
                                        self.__known_address_last_applied
                                    ]

                                    match last_applied_membership_log.command:
                                        case "ADD_NODE":
                                            entries = {
                                                address: ServerInfo(
                                                    len(self.__membership_log) - 1,
                                                    0,
                                                    len(self.__state_log),
                                                    -1,
                                                ) for address in last_applied_membership_log.args
                                            }
                                            self.__current_known_address.update(
                                                entries
                                            )
                                        case "REMOVE_NODE":
                                            for address in last_applied_membership_log.args:
                                                self.__current_known_address.pop(
                                                    address,
                                                    None,
                                                )
                                        case _:
                                            raise RuntimeError(
                                                "Invalid log command"
                                            )

                                    self.__known_address_last_applied += 1
                            except:
                                self.__known_address_last_applied = snapshot_known_address_last_applied
                                self.__current_known_address = snapshot_current_known_address
                                raise RuntimeError(
                                    "Failed to update known address last applied"
                                )
                    except:
                        self.__known_address_commit_index = snapshot_known_address_commit_index
                        raise RuntimeError(
                            "Failed to update known address"
                        )
            except:
                self.__membership_log = snapshot_membership_log
                self.__storage.save_membership_log(
                    self.__membership_log
                )
                raise RuntimeError("Failed to commit log")

    # Public Method (Write)
    # execute enqueue or dequeue
    def add_state(self, command: Literal["ENQUEUE", "DEQUEUE"], message: Tuple[str, ...]) -> None:
        with self.__rw_locks["current_role"].r_locked():
            if self.__current_role != Role.LEADER:
                with self.__rw_locks["current_leader_address"].r_locked():
                    conn = create_connection(
                        self.__current_leader_address
                    )

                    if conn == None:
                        raise RuntimeError("Leader is down")

                    match command:
                        case "ENQUEUE":
                            asyncio.run(
                                dynamically_call_procedure(
                                    conn,
                                    "enqueue",
                                    serialize(message),
                                )
                            )
                        case "DEQUEUE":
                            asyncio.run(
                                dynamically_call_procedure(
                                    conn,
                                    "dequeue",
                                )
                            )
                        case _:
                            raise RuntimeError("Invalid command")
                    return

            with self.__rw_locks["current_term"].r_locked():
                new_state_log = StateLog(
                    self.__current_term,
                    command,
                    message,
                )

                with self.__rw_locks["state_log"].w_locked():
                    snapshot_state_log = copy.deepcopy(
                        self.__state_log
                    )

                    try:
                        self.__state_log.append(new_state_log)

                        with self.__rw_locks["current_known_address"].r_locked(), self.__rw_locks["state_commit_index"].r_locked():
                            known_follower_addresses = {
                                address: server_info
                                for address, server_info in self.__current_known_address.items()
                                if address != self.__config.get("SERVER_ADDRESS")
                            }

                            if len(known_follower_addresses) > 0:
                                list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                ]

                                for address, server_info in known_follower_addresses.items():
                                    conn = create_connection(address)

                                    if conn != None:
                                        list_of_coroutine.append(
                                            dynamically_call_procedure(
                                                conn,
                                                "append_state_logs",
                                                serialize(self.__current_term),
                                                serialize(
                                                    server_info.state_next_index - 1
                                                ),
                                                serialize(
                                                    self.__state_log[
                                                        server_info.state_next_index - 1
                                                    ].term
                                                    if server_info.state_next_index > 0
                                                    else 0
                                                ),
                                                serialize(
                                                    self.__state_log[server_info.state_next_index:]
                                                    if server_info.state_next_index < len(self.__state_log)
                                                    else []
                                                ),
                                                serialize(
                                                    self.__state_commit_index
                                                ),
                                            )
                                        )

                                asyncio.run(
                                    wait_for_majority(
                                        *list_of_coroutine
                                    )
                                )

                            # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                            self.__storage.save_state_log(
                                self.__state_log
                            )
                            with self.__rw_locks["state_commit_index"].r_to_w_locked():
                                snapshot_state_commit_index = copy.deepcopy(
                                    self.__state_commit_index
                                )

                                try:
                                    self.__state_commit_index = len(
                                        self.__state_log
                                    )

                                    with self.__rw_locks["state_last_applied"].w_locked(), self.__rw_locks["state_machine"].w_locked():
                                        snapshot_state_last_applied = copy.deepcopy(
                                            self.__state_last_applied
                                        )
                                        snapshot_state_machine: Queue[str] = Queue(
                                        )
                                        snapshot_state_machine.queue = copy.deepcopy(
                                            self.__state_machine.queue
                                        )

                                        try:
                                            while self.__state_last_applied < self.__state_commit_index:
                                                last_applied_state_log = self.__state_log[
                                                    self.__state_last_applied
                                                ]

                                                match last_applied_state_log.command:
                                                    case "ENQUEUE":
                                                        for msg in last_applied_state_log.args:
                                                            self.__state_machine.put(
                                                                msg
                                                            )
                                                    case "DEQUEUE":
                                                        result = self.__state_machine.get()
                                                        print(
                                                            "Dequeued:",
                                                            result
                                                        )
                                                    case _:
                                                        raise RuntimeError(
                                                            "Invalid log command"
                                                        )

                                                self.__state_last_applied += 1

                                            # Commit and Apply in Current Follower
                                            # Broadcast commit_state_logs to all nodes and wait for majority
                                            if len(known_follower_addresses) > 0:
                                                list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                                                ]

                                                for address, server_info in known_follower_addresses.items():
                                                    conn = create_connection(
                                                        address
                                                    )

                                                    if conn != None:
                                                        list_of_coroutine.append(
                                                            dynamically_call_procedure(
                                                                conn,
                                                                "commit_state_logs",
                                                            )
                                                        )

                                                asyncio.run(
                                                    wait_for_majority(
                                                        *list_of_coroutine
                                                    )
                                                )
                                        except:
                                            self.__state_last_applied = snapshot_state_last_applied
                                            self.__state_machine = snapshot_state_machine
                                            raise RuntimeError(
                                                "Failed to update state machine"
                                            )
                                except:
                                    self.__state_commit_index = snapshot_state_commit_index
                                    raise RuntimeError(
                                        "Failed to update state commit index"
                                    )
                    except:
                        self.__state_log = snapshot_state_log
                        raise RuntimeError("Failed to append state log")

    # Public Method (Write)
    def append_state_logs(self, term: int, prev_log_index: int, prev_log_term: int, new_state_logs: list[StateLog], leader_commit_index: int) -> None:
        # Append in Follower
        with self.__rw_locks["current_term"].r_locked():
            if term < self.__current_term:
                raise RuntimeError("Term is too old")

            with self.__rw_locks["state_log"].r_locked():
                if prev_log_index >= 0 and self.__state_log[prev_log_index].term != prev_log_term:
                    conn = create_connection(self.__current_leader_address)

                    if conn == None:
                        raise RuntimeError("Leader is down")

                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "decrease_next_index",
                            serialize(self.__config.get("SERVER_ADDRESS")),
                        )
                    )
                    return

                temporary_length = len(self.__state_log)
                temporary_index = prev_log_index

                with self.__rw_locks["state_log"].r_to_w_locked():
                    snapshot_state_log = copy.deepcopy(
                        self.__state_log
                    )

                    try:
                        for state_log in new_state_logs:
                            temporary_index += 1

                            if temporary_index < temporary_length:
                                self.__state_log[temporary_index] = state_log
                            else:
                                self.__state_log.append(state_log)

                        final_length = len(self.__state_log)

                        conn = create_connection(self.__current_leader_address)

                        if conn == None:
                            raise RuntimeError("Leader is down")

                        asyncio.run(
                            dynamically_call_procedure(
                                conn,
                                "update_next_match",
                                serialize(self.__config.get("SERVER_ADDRESS")),
                                serialize(final_length),
                                serialize(final_length - 1)
                            )
                        )

                        with self.__rw_locks["state_last_applied"].r_locked():
                            if leader_commit_index <= self.__state_commit_index:
                                return

                            with self.__rw_locks["state_commit_index"].r_to_w_locked():
                                snapshot_state_commit_index = copy.deepcopy(
                                    self.__state_commit_index
                                )

                                try:
                                    self.__state_commit_index = min(
                                        leader_commit_index,
                                        final_length,
                                    )
                                except:
                                    self.__state_commit_index = snapshot_state_commit_index
                                    raise RuntimeError(
                                        "Failed to update known address commit index"
                                    )
                    except:
                        self.__state_log = snapshot_state_log
                        self.__storage.save_state_log(
                            self.__state_log
                        )
                        raise RuntimeError("Failed to append membership logs")

    # Public Method (Write)
    def commit_state_logs(self) -> None:
        # Commit in Follower
        with self.__rw_locks["state_log"].w_locked():
            snapshot_state_log = copy.deepcopy(
                self.__state_log
            )

            try:
                # Write Ahead Logging: Menyimpan log terlebih dahulu sebelum di-apply change
                self.__storage.save_state_log(
                    self.__state_log
                )

                # Apply in Follower
                with self.__rw_locks["state_commit_index"].w_locked():
                    snapshot_state_commit_index = copy.deepcopy(
                        self.__state_commit_index
                    )

                    try:
                        self.__state_commit_index = len(
                            self.__state_log
                        )

                        with self.__rw_locks["state_last_applied"].w_locked(), self.__rw_locks["state_machine"].w_locked():
                            snapshot_state_last_applied = copy.deepcopy(
                                self.__state_last_applied
                            )
                            # Copy state machine
                            snapshot_state_machine: Queue[str] = Queue()
                            snapshot_state_machine.queue = copy.deepcopy(
                                self.__state_machine.queue
                            )
                            try:
                                while self.__state_last_applied < self.__state_commit_index:
                                    last_applied_state_log = self.__state_log[
                                        self.__state_last_applied
                                    ]

                                    match last_applied_state_log.command:
                                        case "ENQUEUE":
                                            for msg in last_applied_state_log.args:
                                                self.__state_machine.put(
                                                    msg
                                                )
                                        case "DEQUEUE":
                                            result = self.__state_machine.get()
                                            print(
                                                "Dequeued:",
                                                result
                                            )
                                        case _:
                                            raise RuntimeError(
                                                "Invalid log command"
                                            )

                                    self.__state_last_applied += 1
                            except:
                                self.__state_last_applied = snapshot_state_last_applied
                                self.__state_machine = snapshot_state_machine
                                raise RuntimeError(
                                    "Failed to update known address last applied"
                                )
                    except:
                        self.__state_commit_index = snapshot_state_commit_index
                        raise RuntimeError(
                            "Failed to update known address"
                        )
            except:
                self.__state_log = snapshot_state_log
                self.__storage.save_state_log(
                    self.__state_log
                )
                raise RuntimeError("Failed to commit log")

    # Decrease next and match index
    def decrease_next_index(self, address: Address) -> None:
        print(f"Decrease next index: {address}")

        with self.__rw_locks["current_known_address"].w_locked():
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                self.__current_known_address[address]._replace(
                    state_next_index=self.__current_known_address[address].state_next_index - 1
                )

                # Sendback append entry to address
                with self.__rw_locks["state_log"].r_locked():
                    conn = create_connection(address)

                    if conn != None:
                        asyncio.run(
                            dynamically_call_procedure(
                                conn,
                                "append_state_logs",
                                serialize(self.__current_term),
                                serialize(
                                    self.__current_known_address[address].state_next_index - 1
                                ),
                                serialize(
                                    self.__state_log[self.__current_known_address[address].state_next_index -
                                                     1].term if self.__current_known_address[address].state_next_index > 0 else 0
                                ),
                                serialize(
                                    self.__state_log[self.__current_known_address[address].state_next_index:]
                                    if self.__current_known_address[address].state_next_index < len(self.__state_log)
                                    else []
                                ),
                                serialize(
                                    self.__state_commit_index
                                ),
                            )
                        )

            except:
                self.__current_known_address = snapshot_current_known_address
                raise RuntimeError("Failed to decrease next index")

    def update_next_match(self, address: Address, next_index: int, match_index: int) -> None:
        with self.__rw_locks["current_known_address"].w_locked():
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                newServerInfo = ServerInfo(
                    self.__current_known_address[address].next_index,
                    self.__current_known_address[address].match_index,
                    next_index,
                    match_index
                )
                self.__current_known_address[address] = newServerInfo

            except:
                self.__current_known_address = snapshot_current_known_address
                raise RuntimeError("Failed to update next index")

    def check_timeout(self) -> None:
        time.sleep(1)

        current_role: Role
        with self.__rw_locks["current_role"].r_locked():
            current_role = self.__current_role

        while True:
            with self.__rw_locks["current_role"].r_locked():
                current_role = self.__current_role

            if current_role != Role.LEADER and time.time() - self.__last_heartbeat_time > self.__heartbeat_timeout:  # TODO: Ini jangan lupa ada lock
                print("Heartbeat Timeout")
                self.start_leader_election()

            # TODO: Ini jangan lupa ada lock
            time.sleep(self.__heartbeat_timeout)

    def start_leader_election(self):
        print("Starting leader election for node")

        with self.__rw_locks["current_role"].w_locked(), self.__rw_locks["current_term"].w_locked():
            snapshot_current_role = copy.deepcopy(
                self.__current_role
            )
            snapshot_current_term = copy.deepcopy(
                self.__current_term
            )

            try:
                self.__current_role = Role.CANDIDATE
                self.__current_term += 1
                self.__storage.save_current_term(self.__current_term)

                print("Current role:", self.__current_role)
                print("Current term:", self.__current_term)

                # Send request vote to all known address
                with self.__rw_locks["current_known_address"].r_locked(), self.__rw_locks["state_commit_index"].r_locked():
                    known_follower_addresses = {
                        address: server_info
                        for address, server_info in self.__current_known_address.items()
                        if address != self.__config.get("SERVER_ADDRESS")
                    }

                    list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                    ]

                    for address, _ in known_follower_addresses.items():
                        conn = create_connection(address)

                        if conn != None:
                            list_of_coroutine.append(
                                dynamically_call_procedure(
                                    conn,
                                    "request_vote",
                                    serialize(self.__current_term),
                                    serialize(
                                        self.__config.get(
                                            "SERVER_ADDRESS"
                                        )
                                    ),
                                    serialize(self.__state_commit_index)
                                )
                            )

                    raw_vote_result = asyncio.run(
                        wait_for_majority(
                            *list_of_coroutine
                        )
                    )

                    vote_result = [
                        bool(deserialize(result))
                        for result in raw_vote_result if result != None
                    ]

                    # Candidate votes for itself
                    if sum(vote_result) >= len(self.__current_known_address) // 2:
                        self.handle_election_win()
                    else:
                        self.__current_role = Role.FOLLOWER
            except:
                self.__current_role = snapshot_current_role
                self.__current_term = snapshot_current_term
                self.__storage.save_current_term(self.__current_term)
                raise RuntimeError("Failed to start leader election")

        # TODO: Ini jangan lupa ada lock dan rollback mechanism
        self.__last_heartbeat_time = time.time()
        self.__heartbeat_timeout = random.uniform(2.0, 3.0)

    def become_follower(self, address, term):
        print("Become follower")

        with self.__rw_locks["current_leader_address"].w_locked(), self.__rw_locks["current_role"].w_locked(), self.__rw_locks["current_term"].w_locked():
            snapshot_current_leader_address = copy.deepcopy(
                self.__current_leader_address
            )
            snapshot_current_role = copy.deepcopy(
                self.__current_role
            )
            snapshot_current_term = copy.deepcopy(
                self.__current_term
            )

            try:
                self.__current_leader_address = address
                self.__current_role = Role.FOLLOWER
                self.__current_term = term
                self.__storage.save_current_term(self.__current_term)
                self.__storage.save_current_leader_address(
                    self.__current_leader_address
                )

                print(
                    "Current leader address: ",
                    self.__current_leader_address
                )
                print("Current role: ", self.__current_role)
                print("Current term: ", self.__current_term)

            except:
                self.__current_leader_address = snapshot_current_leader_address
                self.__current_role = snapshot_current_role
                self.__current_term = snapshot_current_term
                self.__storage.save_current_term(self.__current_term)
                self.__storage.save_current_leader_address(
                    self.__current_leader_address
                )
                raise RuntimeError("Failed to become follower")

        # TODO: Ini jangan lupa ada lock dan rollback mechanism
        self.__last_heartbeat_time = time.time()
        self.__heartbeat_timeout = random.uniform(1.5, 3.0)

    # TODO: Ini jangan lupa ada lock dan rollback mechanism
    def handle_election_win(self):
        print("Election won by node")

        with self.__rw_locks["current_role"].w_locked(), self.__rw_locks["current_leader_address"].w_locked():
            snapshot_current_role = copy.deepcopy(
                self.__current_role
            )
            snapshot_current_leader_address = copy.deepcopy(
                self.__current_leader_address
            )

            try:
                self.__current_role = Role.LEADER
                self.__current_leader_address = self.__config.get(
                    "SERVER_ADDRESS"
                )
                self.__storage.save_current_leader_address(
                    self.__current_leader_address
                )

                print("Current role: ", self.__current_role)
                print(
                    "Current leader address: ",
                    self.__current_leader_address
                )

                # RPC become follower to all known address
                with self.__rw_locks["current_known_address"].r_locked(), self.__rw_locks["current_term"].r_locked():
                    known_follower_addresses = {
                        address: server_info
                        for address, server_info in self.__current_known_address.items()
                        if address != self.__config.get("SERVER_ADDRESS")
                    }

                    list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
                    ]

                    for address, _ in known_follower_addresses.items():
                        conn = create_connection(address)

                        if conn != None:
                            list_of_coroutine.append(
                                dynamically_call_procedure(
                                    conn,
                                    "become_follower",
                                    serialize(self.__current_term),
                                    serialize(self.__current_leader_address),
                                )
                            )

                    asyncio.run(
                        wait_for_majority(
                            *list_of_coroutine
                        )
                    )

            except:
                self.__current_role = snapshot_current_role
                self.__current_leader_address = snapshot_current_leader_address
                self.__storage.save_current_leader_address(
                    self.__current_leader_address
                )
                raise RuntimeError("Failed to handle election win")

        # TODO: Ini jangan lupa ada lock dan rollback mechanism
        self.__heartbeat_thread = threading.Thread(target=self.start_heartbeat)
        self.__heartbeat_thread.daemon = True
        self.__heartbeat_thread.start()

    def request_vote(self, term: int, candidate_address: Address, state_commit_index: int) -> bool:
        with self.__rw_locks["current_term"].r_locked(), self.__rw_locks["current_role"].r_locked(), self.__rw_locks["state_commit_index"].r_locked():
            if term < self.__current_term or self.__current_role == Role.CANDIDATE or state_commit_index < self.__state_commit_index:
                print("Vote rejected for:", candidate_address)
                return False

            with self.__rw_locks["current_term"].r_to_w_locked(), self.__rw_locks["voted_for"].w_locked():
                snapshot_current_term = copy.deepcopy(
                    self.__current_term
                )
                snapshot_voted_for = copy.deepcopy(
                    self.__voted_for
                )

                try:
                    self.__current_term = term
                    self.__storage.save_current_term(self.__current_term)

                    self.__voted_for = candidate_address
                    self.__storage.save_voted_for(self.__voted_for)

                    print("Current term:", self.__current_term)
                    print("Voted for:", self.__voted_for)

                    # TODO: Ini jangan lupa ada lock dan rollback mechanism
                    self.__last_heartbeat_time = time.time()
                    self.__heartbeat_timeout = random.uniform(2.0, 3.0)

                    return True

                except:
                    self.__current_term = snapshot_current_term
                    self.__voted_for = snapshot_voted_for
                    self.__storage.save_current_term(self.__current_term)
                    self.__storage.save_voted_for(self.__voted_for)
                    raise RuntimeError("Failed to vote")

    def send_heartbeat(self):
        with self.__rw_locks["current_known_address"].r_locked(), self.__rw_locks["current_term"].r_locked():
            known_follower_addresses = {
                address: server_info
                for address, server_info in self.__current_known_address.items()
                if address != self.__config.get("SERVER_ADDRESS")
            }

            list_of_coroutine: list[Coroutine[Any, Any, Optional[bytes]]] = [
            ]

            for address, _ in known_follower_addresses.items():
                conn = create_connection(address)

                if conn != None:
                    list_of_coroutine.append(
                        dynamically_call_procedure(
                            conn,
                            "handle_heartbeat",
                            serialize(self.__current_term),
                            serialize(self.__config.get("SERVER_ADDRESS")),
                        )
                    )

            asyncio.run(
                wait_for_majority(
                    *list_of_coroutine
                )
            )

    def start_heartbeat(self):
        current_role: Role
        with self.__rw_locks["current_role"].r_locked():
            current_role = self.__current_role

        while current_role == Role.LEADER:
            self.send_heartbeat()
            time.sleep(self.__heartbeat_interval)

            with self.__rw_locks["current_role"].r_locked():
                current_role = self.__current_role

    def handle_heartbeat(self, term: int, address: Address):
        # If received heartbeat and is leader, check is received term greater than current term
        # If greater, become follower and update current term

        with self.__rw_locks["current_term"].r_locked():
            if term > self.__current_term:
                print("Stepping down")

                with self.__rw_locks["current_term"].r_to_w_locked(), self.__rw_locks["current_role"].w_locked():
                    snapshot_current_term = copy.deepcopy(
                        self.__current_term
                    )
                    snapshot_current_role = copy.deepcopy(
                        self.__current_role
                    )

                    try:
                        self.__current_term = term
                        self.__storage.save_current_term(self.__current_term)
                        self.__current_role = Role.FOLLOWER

                        with self.__rw_locks["current_leader_address"].r_locked():
                            if address != self.__current_leader_address and term >= self.__current_term:

                                with self.__rw_locks["current_leader_address"].r_to_w_locked():
                                    snapshot_current_leader_address = copy.deepcopy(
                                        self.__current_leader_address
                                    )

                                    try:
                                        self.__current_leader_address = address
                                        self.__storage.save_current_leader_address(
                                            self.__current_leader_address
                                        )
                                    except:
                                        self.__current_leader_address = snapshot_current_leader_address
                                        self.__storage.save_current_leader_address(
                                            self.__current_leader_address
                                        )
                                        raise RuntimeError(
                                            "Failed to update current leader address"
                                        )

                    except:
                        self.__current_term = snapshot_current_term
                        self.__current_role = snapshot_current_role
                        self.__storage.save_current_term(self.__current_term)
                        raise RuntimeError("Failed to update current term")

        # TODO: Ini jangan lupa ada lock dan rollback mechanism
        self.__last_heartbeat_time = time.time()


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __node: RaftNode
    __conn: rpyc.Connection  # Two Way Communication

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__node = RaftNode()
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    @rpyc.exposed
    def add_server(self, raw_follower_address: bytes) -> None:
        follower_addresses: Tuple[Address, ...] = deserialize(
            raw_follower_address
        )

        self.__node.add_server(follower_addresses)

    @rpyc.exposed
    def append_membership_logs(self, raw_term: bytes, raw_prev_log_index: bytes, raw_prev_log_term: bytes, raw_new_membership_logs: bytes, raw_leader_commit_index: bytes) -> None:
        term: int = deserialize(raw_term)
        prev_log_index: int = deserialize(raw_prev_log_index)
        prev_log_term: int = deserialize(raw_prev_log_term)
        new_membership_logs: list[MembershipLog] = deserialize(
            raw_new_membership_logs
        )
        leader_commit_index: int = deserialize(raw_leader_commit_index)

        self.__node.append_membership_logs(
            term,
            prev_log_index,
            prev_log_term,
            new_membership_logs,
            leader_commit_index,
        )

    @rpyc.exposed
    def commit_membership_logs(self) -> None:
        self.__node.commit_membership_logs()

    @rpyc.exposed
    def enqueue(self, raw_message: bytes) -> None:
        message: Tuple[str, ...] = deserialize(raw_message)

        self.__node.add_state("ENQUEUE", message)

    @rpyc.exposed
    def dequeue(self) -> None:
        self.__node.add_state("DEQUEUE", ())

    @rpyc.exposed
    def append_state_logs(self, raw_term: bytes, raw_prev_log_index: bytes, raw_prev_log_term: bytes, raw_new_state_logs: bytes, raw_leader_commit_index: bytes) -> None:
        term: int = deserialize(raw_term)
        prev_log_index: int = deserialize(raw_prev_log_index)
        prev_log_term: int = deserialize(raw_prev_log_term)
        new_state_logs: list[StateLog] = deserialize(
            raw_new_state_logs
        )
        leader_commit_index: int = deserialize(raw_leader_commit_index)

        self.__node.append_state_logs(
            term,
            prev_log_index,
            prev_log_term,
            new_state_logs,
            leader_commit_index,
        )

    @rpyc.exposed
    def decrease_next_index(self, raw_address: bytes) -> None:
        address: Address = deserialize(raw_address)

        self.__node.decrease_next_index(address)

    @rpyc.exposed
    def update_next_match(self, raw_address: bytes, raw_next_index: bytes, raw_match_index: bytes) -> None:
        address: Address = deserialize(raw_address)
        next_index: int = deserialize(raw_next_index)
        match_index: int = deserialize(raw_match_index)

        self.__node.update_next_match(address, next_index, match_index)

    @rpyc.exposed
    def commit_state_logs(self) -> None:
        self.__node.commit_state_logs()

    @rpyc.exposed
    def handle_heartbeat(self, raw_term: bytes, raw_address: bytes) -> None:
        term: int = deserialize(raw_term)
        address: Address = deserialize(raw_address)

        self.__node.handle_heartbeat(term, address)

    @rpyc.exposed
    def request_vote(self, raw_term: bytes, raw_candidate_address: bytes, raw_state_commit_index: bytes) -> bytes:
        term: int = deserialize(raw_term)
        candidate_address: Address = deserialize(raw_candidate_address)
        state_commit_index: int = deserialize(raw_state_commit_index)

        return serialize(self.__node.request_vote(term, candidate_address, state_commit_index))

    @rpyc.exposed
    def become_follower(self, raw_term: bytes, raw_leader_address: bytes) -> None:
        term: int = deserialize(raw_term)
        leader_address: Address = deserialize(raw_leader_address)

        self.__node.become_follower(leader_address, term)

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_node(self) -> None:
        print("Current known address", self.__node.get_current_known_address())
        print(
            "Known Address Commit Index:",
            self.__node.get_known_address_commit_index()
        )
        print(
            "Known Address Last Applied:",
            self.__node.get_known_address_last_applied()
        )
        print("Leader Address:", self.__node.get_leader_address())
        print("Current Term:", self.__node.get_current_term())
        print("Current Role:", self.__node.get_current_role())
        print("Current State Machine:", self.__node.get_state_machine())
        print(
            "Current State Commit Index:",
            self.__node.get_state_commit_index()
        )
        print(
            "Current State Last Applied:",
            self.__node.get_state_last_applied()
        )
        print("Current State Log:", self.__node.get_state_log())
