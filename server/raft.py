import time
from threading import Lock
from sched import scheduler
from queue import Queue
from log import Log
from state import State
from address import Address
from storage import Storage
from config import ServerConfig


class RaftNodeMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RaftNode(metaclass=RaftNodeMeta):  # TODO: Implementasikan kelas RaftNode
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
    _current_known_address: list[Address] = []  # Ditentukan saat init
    _current_leader_address: Address = _config.get("LEADER_ADDRESS")
