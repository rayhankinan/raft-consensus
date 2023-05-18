import time
import copy
import asyncio
import rpyc
from threading import Lock
from sched import scheduler
from queue import Queue
from typing import Tuple
from data import Address, ServerInfo, MembershipLog, StateLog, Role
from . import Storage, ServerConfig, RWLock, dynamically_call_procedure, wait_for_majority, wait_for_all, serialize, deserialize


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

    # Public Method (Read): Testing untuk client
    def get_current_known_address(self) -> dict[Address, ServerInfo]:
        with self.__rw_locks["current_known_address"].r_locked():
            return self.__current_known_address

    # Public Method (Read): Testing untuk client
    def get_membership_log(self) -> list[MembershipLog]:
        with self.__rw_locks["membership_log"].r_locked():
            return self.__membership_log

    # Public Method (Write)
    def start(self) -> None:
        with self.__rw_locks["current_leader_address"].r_locked():
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
                            self.__membership_log.append(new_membership_log)

                            with self.__rw_locks["known_address_commit_index"].w_locked():
                                snapshot_known_address_commit_index = copy.deepcopy(
                                    self.__known_address_commit_index
                                )

                                try:
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
                                            )
                                            entries = {
                                                current_address: current_server_info
                                            }

                                            self.__current_known_address.update(
                                                entries
                                            )
                                            self.__known_address_commit_index += 1
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

    # TODO: Implementasikan penghapusan node dari cluster
    # Public Method (Write)
    def stop(self) -> None:
        pass

    # Public Method (Write)
    def add_server(self, follower_addresses: Tuple[Address, ...]) -> None:
        with self.__rw_locks["current_role"].r_locked():
            if self.__current_role != Role.LEADER:
                with self.__rw_locks["current_leader_address"].r_locked():
                    conn = create_connection(
                        self.__current_leader_address
                    )
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
                                asyncio.run(
                                    wait_for_majority(
                                        *(
                                            dynamically_call_procedure(
                                                create_connection(address),
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
                                            for address, server_info in known_follower_addresses.items()
                                        )
                                    )
                                )

                                # TODO: Update nilai next_index

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
                                                asyncio.run(
                                                    wait_for_majority(
                                                        *(
                                                            dynamically_call_procedure(
                                                                create_connection(
                                                                    address
                                                                ),
                                                                "commit_membership_logs",
                                                            )
                                                            for address, _ in new_known_follower_addresses.items()
                                                        )
                                                    )
                                                )

                                                # TODO: Update nilai match_index

                                            # Append in New Follower
                                            asyncio.run(
                                                wait_for_all(
                                                    *(
                                                        dynamically_call_procedure(
                                                            create_connection(
                                                                follower_address
                                                            ),
                                                            "append_membership_logs",
                                                            serialize(
                                                                self.__current_term
                                                            ),
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
                                                        for follower_address in follower_addresses
                                                    )
                                                )
                                            )

                                            # Commit in New Follower
                                            asyncio.run(
                                                wait_for_all(
                                                    *(
                                                        dynamically_call_procedure(
                                                            create_connection(
                                                                follower_address
                                                            ),
                                                            "commit_membership_logs",
                                                        )
                                                        for follower_address in follower_addresses
                                                    )
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

    # Procedure
    @rpyc.exposed
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

    # Procedure
    @rpyc.exposed
    def commit_membership_logs(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        self.__node.commit_membership_logs()

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
