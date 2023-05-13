import rpyc
import asyncio
import time
from threading import Lock
from sched import scheduler
from queue import Queue
from . import Log, State, Address, Storage, ServerConfig, dynamically_call_procedure, serialize, deserialize


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
    _scheduler: scheduler = scheduler(time.time, time.sleep)
    _storage: Storage = Storage()
    _config: ServerConfig = ServerConfig()

    # Persistent state on all servers
    _logs: list[Log] = _storage.get_logs()
    _current_term: int = _storage.get_current_term()
    _voted_for: Address = _storage.get_voted_for()

    # Volatile state on all servers
    _state_machine: Queue[str] = Queue()
    _commit_index: int = 0
    _last_applied: int = 0
    _current_state: State = State.FOLLOWER
    _current_known_address: list[Address] = []
    _current_leader_address: Address = _config.get("LEADER_ADDRESS")

    def initialize(self) -> None:
        if self._current_leader_address == self._config.get("SERVER_ADDRESS"):
            self._current_state = State.LEADER
            self._current_known_address.append(
                self._config.get("SERVER_ADDRESS")
            )

        else:
            hostname, port = self._current_leader_address
            # TODO: BUG DISINI
            conn: rpyc.Connection = rpyc.connect(
                hostname,
                port,
                service=ServerService,
            )

            asyncio.run(dynamically_call_procedure(conn, "apply_membership"))

    def get_current_address(self) -> Address:
        return self._config.get("SERVER_ADDRESS")

    def get_current_term(self) -> int:
        return self._current_term

    def add_log(self, log: Log) -> None:
        self._logs.append(log)

    def commit_log(self) -> None:
        self._storage.save_logs(self._logs)
        self._commit_index = len(self._logs) - 1

    def apply_log(self) -> None:
        while self._last_applied < self._commit_index:
            self._last_applied += 1

            last_applied_log = self._logs[self._last_applied]
            match last_applied_log.command:
                case "ENQUEUE":
                    raw_value, = last_applied_log.args
                    value = str(raw_value)
                    self._state_machine.put(value)
                case "DEQUEUE":
                    self._state_machine.get()
                case "ADD_NODE":
                    raw_hostname, raw_port = last_applied_log.args
                    hostname, port = str(raw_hostname), int(raw_port)
                    self._current_known_address.append(Address(hostname, port))
                case "REMOVE_NODE":
                    raw_hostname, raw_port = last_applied_log.args
                    hostname, port = str(raw_hostname), int(raw_port)
                    self._current_known_address.remove(Address(hostname, port))
                case _:
                    raise RuntimeError("Unknown command")


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    _node: RaftNode
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection) -> None:
        self._node = RaftNode()
        self._conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    @rpyc.exposed
    def apply_membership(self) -> None:
        follower_address: Address = deserialize(
            asyncio.run(
                dynamically_call_procedure(
                    self._conn,
                    "get_current_address",
                )
            )
        )

        new_log = Log(
            self._node.get_current_term(),
            "ADD_NODE",
            follower_address,
        )

        self._node.add_log(new_log)
        # TODO: Broadcast to all nodes and wait for majority

        self._node.commit_log()
        self._node.apply_log()
        # TODO: Broadcast to all nodes and wait for majority

    @rpyc.exposed
    def get_current_address(self) -> bytes:
        return serialize(self._node.get_current_address())

    @rpyc.exposed
    def print_logs(self) -> None:  # Test untuk client
        print(self._node._logs)
