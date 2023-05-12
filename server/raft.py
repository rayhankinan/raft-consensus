import time
from sched import scheduler
from queue import Queue
from log import Log
from meta import ThreadSafeSingletonMeta
from config import ServerConfig
from state import State
from storage import Storage


class RaftNode(metaclass=ThreadSafeSingletonMeta):  # TODO: Implementasikan kelas RaftNode
    _state_machine: Queue[str]
    _scheduler: scheduler
    _logs: list[Log]
    _current_term: int
    _current_state: State
    _voted_for: str

    def __init__(self):
        config = ServerConfig()

        self._state_machine = Queue()
        self._scheduler = scheduler(time.time, time.sleep)
        self._logs = []
        self._current_term = 0
        self._current_state = State.FOLLOWER
        self._voted_for = config.get("HOSTNAME")
