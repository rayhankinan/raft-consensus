import time
from sched import scheduler
from queue import Queue
from log import Log
from meta import ThreadSafeSingletonMeta
from state import State
from storage import Storage


class RaftNode(metaclass=ThreadSafeSingletonMeta):  # TODO: Implementasikan kelas RaftNode
    # Persistent state on all servers
    _logs: list[Log]
    _current_term: int
    _voted_for: str

    # Volatile state on all servers
    _state_machine: Queue[str]
    _current_state: State
    _commit_index: int
    _last_applied: int

    # Utility
    _scheduler: scheduler

    def __init__(self) -> None:
        storage = Storage()

        self._logs = storage.get_logs()
        self._current_term = storage.get_current_term()
        self._voted_for = storage.get_voted_for()

        self._state_machine = Queue()
        self._current_state = State.FOLLOWER
        self._commit_index = 0
        self._last_applied = 0

        self._scheduler = scheduler(time.time, time.sleep)
