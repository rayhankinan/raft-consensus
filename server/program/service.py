import time
import copy
import asyncio
import rpyc
from threading import Lock
from sched import scheduler
from queue import Queue
from typing import Literal, Tuple
from data import Address, ServerInfo, MembershipLog, StateLog, Role
from . import Storage, ServerConfig, RWLock, dynamically_call_procedure, wait_for_majority, wait_for_all, serialize, deserialize
import threading
import random

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

    # Hearbeat
    #randomize timeout
    __heartbeat_timeout: float = random.uniform(0.15, 0.3)
    __last_heartbeat_time = time.time()


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
            return self.__state_machine
    
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
                asyncio.run(
                    dynamically_call_procedure(
                        conn,
                        "add_server",
                        serialize(server_addresses),
                    )
                )

                self.start_timer()
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
                            print(f"Adding default leader {self.__config.get('SERVER_ADDRESS')} to membership log")
                            self.__membership_log.append(new_membership_log)

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
                                                0,
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
                
        #heartbeat
        self.start_heartbeat()
        
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

                                # TODO: Update nilai next_index (bisa pake lambda function)

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

                                                # TODO: Update nilai match_index (bisa pake lambda function)

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
                                                    len(self.__state_log),
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
    
    # Public Method (Write)
    # execute enqueue or dequeue
    def add_state(self, command: Literal["ENQUEUE", "DEQUEUE"], message: Tuple[str,...]) -> None:
        with self.__rw_locks["current_role"].r_locked():
            if self.__current_role != Role.LEADER:
                with self.__rw_locks["current_leader_address"].r_locked():
                    conn = create_connection(
                        self.__current_leader_address
                    )
                    if (command == "ENQUEUE"):
                        asyncio.run(
                            dynamically_call_procedure(
                                conn,
                                "enqueue",
                                serialize(message),
                            )
                        )
                    else:
                        asyncio.run(
                            dynamically_call_procedure(
                                conn,
                                "dequeue",
                            )
                        )
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
                                asyncio.run(
                                    wait_for_majority(
                                        *(
                                            dynamically_call_procedure(
                                                create_connection(address),
                                                "append_state_logs",
                                                serialize(self.__current_term),
                                                serialize(
                                                    server_info.state_next_index - 1
                                                ),
                                                serialize(        
                                                    self.__state_log[server_info.state_next_index - 1].term if server_info.state_next_index > 0 else 0
                                                ),
                                                serialize(
                                                    self.__state_log[server_info.state_next_index:]
                                                ),
                                                serialize(
                                                    self.__state_commit_index
                                                ),
                                            )
                                            for address, server_info in known_follower_addresses.items()
                                        )
                                    )
                                )

                                # TODO: Update nilai next_index (bisa pake lambda function)

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
                                        snapshot_state_machine = copy.deepcopy(
                                            self.__state_machine
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
                                                        self.__state_machine.get()
                                                    case _:
                                                        raise RuntimeError(
                                                            "Invalid log command")

                                                self.__state_last_applied += 1

                                            # Commit and Apply in Current Follower
                                            # Broadcast commit_state_logs to all nodes and wait for majority
                                            if len(known_follower_addresses) > 0:
                                                asyncio.run(
                                                    wait_for_majority(
                                                        *(
                                                            dynamically_call_procedure(
                                                                create_connection(
                                                                    address
                                                                ),
                                                                "commit_state_logs",
                                                            )
                                                            for address, _ in known_follower_addresses.items()
                                                        )
                                                    )
                                                )
                                                # TODO: Update nilai match_index (bisa pake lambda function)

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
                if prev_log_index > 0 and self.__state_log[prev_log_index].term != prev_log_term:
                    # Kurangi nilai prev_log_index pada RPC yang dipanggil oleh leader dan ulangi lagi
                    # Dikarenakan byzantine leader, maka follower harus mengecek apakah prev_log_index yang dikirimkan oleh leader valid
                    raise RuntimeError("Prev Log Term does not match")

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
                            snapshot_state_machine = copy.deepcopy(
                                self.__state_machine
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
                                            self.__state_machine.get()
                                        case _:
                                            raise RuntimeError(
                                                "Invalid log command")

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
    

    # Heartbeat
    def check_heartbeat_timeout(self) :
        # wait 1 second
        time.sleep(1)
        while True :
            current_time = time.time()
            elapsed_time = current_time - self.__last_heartbeat_time

            if(elapsed_time > self.__heartbeat_timeout) :
                self.handle_leadership_timeout()

            time.sleep(self.__heartbeat_timeout)
    
    def start_timer(self) :
        #print("Starting timer")
        #sleep for 1 second
        time.sleep(1)
        timer_thread = threading.Thread(target=self.check_heartbeat_timeout)
        timer_thread.daemon = True
        timer_thread.start()

    def handle_leadership_timeout(self):
        print("Leadership timeout")

        self.start_leader_election()

    def start_leader_election(self):
        print("Starting leader election")

        # change current role to candidate
        with self.__rw_locks["current_role"].w_locked() :
            self.__current_role = Role.CANDIDATE
            
        
        with self.__rw_locks["current_term"].w_locked(), self.__rw_locks["voted_for"].w_locked() :
            self.__current_term += 1
            self.__voted_for = self.__config.get("SERVER_ADDRESS")

        votes_received = 1
        total_nodes = len(self.__current_known_address)
        majority = total_nodes // 2 + 1

        # send request vote to all known address
        with self.__rw_locks["current_known_address"].r_locked() :
            for address in self.__current_known_address :
                # skip if address is current server address
                if(address == self.__config.get("SERVER_ADDRESS")) :
                    continue
                
                # send request vote to address
                conn = create_connection(address)
                try :
                    vote_result = asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "request_vote",
                            serialize(self.__current_term),
                            serialize(self.__config.get("SERVER_ADDRESS")),
                            
                        )
                    )
                    if vote_result :
                        votes_received += 1
                        if(votes_received >= majority) :
                            self.handle_election_win()
                            return
                except :
                    print("Failed to request vote to {}".format(address))
                    continue

        # change current role to follower
        with self.__rw_locks["current_role"].w_locked() :
            self.__current_role = Role.FOLLOWER

                
    
    def handle_election_win(self):
        print("Election won")
        self.__current_role = Role.LEADER
        
    
    def request_vote(self, term, candidate_id) :
        with self.__rw_locks["current_term"].r_locked(), self.__rw_locks["voted_for"].w_locked():
            current_term = self.__current_term
            voted_for = self.__voted_for

            if(term < current_term) :
                return False
            else :
                self.__current_term = term
                

                if(voted_for is None) :
                    self.__voted_for = candidate_id
                    return True
                else :  
                    return False
                
            

    def handle_heartbeat(self):
        print("Heartbeat received")
        self.__last_heartbeat_time = time.time()
        

    def hearbeat_loop(self):
        while True :
            elapsed_time = time.time() - self.__last_heartbeat_time
            if(elapsed_time > 0.1) :
                self.send_heartbeat()

                self.__last_heartbeat_time = time.time()

    def send_heartbeat(self):
        #print("Sending heartbeat")
        
        # loop through all known address
        with self.__rw_locks["current_known_address"].r_locked() :
            for address in self.__current_known_address :
                # skip if address is current server address
                    if(address == self.__config.get("SERVER_ADDRESS")) :
                        continue
                    
                    # send heartbeat to address
                    conn = create_connection(address)
                    asyncio.run(
                        dynamically_call_procedure(
                            conn,
                            "handle_heartbeat",
                        )
                    )
            

    def start_heartbeat(self):
        print("Starting heartbeat")
        heartbeat_thread = threading.Thread(target=self.hearbeat_loop)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

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
            term, 
            prev_log_index,
            prev_log_term,
            new_membership_logs,
            leader_commit_index,
        )

    # Procedure
    @rpyc.exposed
    def commit_membership_logs(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        self.__node.commit_membership_logs()

    # Procedure
    @rpyc.exposed
    def enqueue(self, raw_message: bytes) -> None:
        message: Tuple[str, ...] = deserialize(raw_message)
        self.__node.add_state("ENQUEUE", message)

    # Procedure
    @rpyc.exposed
    def dequeue(self) -> None:
        self.__node.add_state("DEQUEUE",  ())

    # Procedure
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

    # Procedure
    @rpyc.exposed
    def commit_state_logs(self) -> None:
        self.__node.commit_state_logs()

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
        
	# Procedure: Test untuk client
    @rpyc.exposed
    def print_node(self) -> None:
        print("current known address", self.__node.get_current_known_address())
        print("Known Address Commit Index:", self.__node.get_known_address_commit_index())
        print("Known Address Last Applied:", self.__node.get_known_address_last_applied())
        print("Leader Address:", self.__node.get_leader_address())
        print("Current Term:", self.__node.get_current_term())
        print("Current Role:", self.__node.get_current_role())
        print("Current State Machine:", self.__node.get_state_machine())
        print("Current State Commit Index:", self.__node.get_state_commit_index())
        print("Current State Last Applied:", self.__node.get_state_last_applied())
        print("Current State Log:", self.__node.get_state_log())

    @rpyc.exposed
    def handle_heartbeat(self) -> None:
        self.__node.handle_heartbeat()
    
    @rpyc.exposed
    def request_vote(self, raw_term: bytes, raw_candidate_address: bytes) -> None:
        term: int = deserialize(raw_term)
        candidate_address: Address = deserialize(raw_candidate_address)


        self.__node.request_vote(term, candidate_address)