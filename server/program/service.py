import time
import copy
import asyncio
import rpyc
from threading import Lock
from sched import scheduler
from queue import Queue
from typing import Tuple
from data import Address, ServerInfo, MembershipLog, StateLog, Role
from . import Storage, ServerConfig, RWLock, dynamically_call_procedure, serialize, deserialize


def create_connection(address: Address) -> rpyc.Connection:
    hostname, port = address
    conn = rpyc.connect(
        hostname,
        port,
        service=ServerService,
    )

    if type(conn) != rpyc.Connection:
        raise RuntimeError(f"Failed to connect to {hostname}:{port}")

    return conn


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

    # Public Method (Read)
    def get_membership_log(self) -> list[MembershipLog]:
        with self.__rw_locks["membership_log"].r_locked():
            return self.__membership_log

    # Public Method (Write)
    def start(self) -> None:
        with self.__rw_locks["current_leader_address"].r_locked(), self.__rw_locks["membership_log"].r_locked(), self.__rw_locks["current_role"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
            snapshot_current_state = copy.deepcopy(
                self.__current_role
            )
            snapshot_current_known_address = copy.deepcopy(
                self.__current_known_address
            )

            try:
                if self.__current_leader_address != self.__config.get("SERVER_ADDRESS"):
                    conn = create_connection(self.__current_leader_address)
                    server_addresses = (
                        self.__config.get("SERVER_ADDRESS"),
                    )
                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "add_server",
                            serialize(server_addresses),
                        )
                    )
                    return

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

    # TODO: Implementasikan penghapusan node dari cluster
    # Public Method (Write)
    def stop(self) -> None:
        pass

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

    # Public Method (Write)
    def add_server(self, follower_addresses: Tuple[Address, ...]) -> None:
        with self.__rw_locks["current_role"].r_locked(), self.__rw_locks["current_leader_address"].r_locked(), self.__rw_locks["current_term"].r_locked(), self.__rw_locks["membership_log"].w_locked(), self.__rw_locks["known_address_commit_index"].w_locked(), self.__rw_locks["known_address_last_applied"].w_locked(), self.__rw_locks["current_known_address"].w_locked():
            snapshot_membership_log = copy.deepcopy(self.__membership_log)
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
                if self.__current_role != Role.LEADER:
                    conn = create_connection(self.__current_leader_address)
                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "add_server",
                            serialize(follower_addresses),
                        )
                    )
                    return

                new_membership_log = MembershipLog(
                    self.__current_term,
                    "ADD_NODE",
                    follower_addresses,
                )
                self.__membership_log.append(new_membership_log)

                # TODO: Broadcast append_membership_logs to all nodes and wait for majority

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

                # TODO: Broadcast commit_membership_logs to all nodes and wait for majority

            except:
                self.__membership_log = snapshot_membership_log
                self.__known_address_commit_index = snapshot_known_address_commit_index
                self.__known_address_last_applied = snapshot_known_address_last_applied
                self.__current_known_address = snapshot_current_known_address

                self.__storage.save_membership_log(self.__membership_log)
                raise RuntimeError("Failed to add server")

    # Public Method (Write)
    def append_membership_logs(self, term: int, prev_log_index: int, prev_log_term: int, new_membership_logs: list[MembershipLog], leader_commit_index: int) -> None:
        with self.__rw_locks["current_term"].r_locked(), self.__rw_locks["membership_log"].w_locked(), self.__rw_locks["known_address_commit_index"].w_locked():
            snapshot_membership_log = copy.deepcopy(self.__membership_log)
            snapshot_known_address_commit_index = copy.deepcopy(
                self.__known_address_commit_index
            )

            temporary_index = prev_log_index
            temporary_length = len(self.__membership_log)

            if term < self.__current_term:
                raise RuntimeError("Term is too old")

            if self.__membership_log[prev_log_index].term != prev_log_term:
                # Kurangi nilai prev_log_index pada RPC yang dipanggil oleh leader dan ulangi lagi
                raise RuntimeError("Prev Log Term does not match")

            try:
                for membership_log in new_membership_logs:
                    temporary_index += 1

                    if temporary_index < temporary_length:
                        self.__membership_log[temporary_index] = membership_log
                    else:
                        self.__membership_log.append(membership_log)

                final_length = len(self.__membership_log)
                self.__known_address_commit_index = min(
                    leader_commit_index,
                    final_length - 1,
                )
            except:
                self.__membership_log = snapshot_membership_log
                self.__known_address_commit_index = snapshot_known_address_commit_index
                raise RuntimeError("Failed to append membership logs")


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __node: RaftNode
    __conn: rpyc.Connection  # Two Way Communication

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__node = RaftNode()
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    # Procedure
    @rpyc.exposed
    def add_server(self, raw_follower_address: bytes) -> None:
        # NOTE: Dalam satu flow service hanya boleh terpanggil satu method pada node (menjaga atomicity)

        follower_addresses: Tuple[Address, ...] = deserialize(
            raw_follower_address
        )

        self.__node.add_server(follower_addresses)

    # TODO: Masih Untested
    # Procedure
    def append_membership_logs(self, raw_term: bytes, raw_prev_log_index: bytes, raw_prev_log_term: bytes, raw_new_membership_logs: bytes, raw_leader_commit_index: bytes) -> None:
        term: int = deserialize(raw_term)
        prev_log_index: int = deserialize(raw_prev_log_index)
        prev_log_term: int = deserialize(raw_prev_log_term)
        new_membership_logs: list[MembershipLog] = deserialize(
            raw_new_membership_logs
        )
        leader_commit_index: int = deserialize(raw_leader_commit_index)

        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        self.__node.append_membership_logs(
            term, prev_log_index,
            prev_log_term,
            new_membership_logs,
            leader_commit_index,
        )

    # TODO: Masih Untested
    # Procedure
    def commit_membership_logs(self) -> None:
        pass

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_membership_log(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        print("Membership Logs:", self.__node.get_membership_log())

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        print("Known Address:", self.__node.get_current_known_address())
