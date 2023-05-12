import time
from sched import scheduler
from queue import Queue
from log import Log
from meta import ThreadSafeSingletonMeta
from state import State
from storage import Storage


class RaftNode(metaclass=ThreadSafeSingletonMeta):  # TODO: Implementasikan kelas RaftNode
    # Utility
    _scheduler: scheduler = scheduler(time.time, time.sleep)
    _storage: Storage = Storage()

    # Persistent state on all servers
    _logs: list[Log] = _storage.get_logs()
    _current_term: int = _storage.get_current_term()
    _voted_for: str = _storage.get_voted_for()

    # Volatile state on all servers
    _state_machine: Queue[str] = Queue()
    _current_state: State = State.FOLLOWER
    _commit_index: int = 0
    _last_applied: int = 0
