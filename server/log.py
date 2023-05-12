from typing import Literal, Tuple, NamedTuple


class Log(NamedTuple):
    term: int
    command: Literal["ENQUEUE", "DEQUEUE"]
    args: Tuple[str, ...]
