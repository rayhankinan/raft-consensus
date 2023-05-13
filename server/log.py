from typing import Literal, Tuple, NamedTuple, Any


class Log(NamedTuple):
    term: int
    command: Literal["ENQUEUE", "DEQUEUE", "ADD_NODE", "REMOVE_NODE"]
    args: Tuple[Any, ...]
