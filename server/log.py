from typing import Literal, Tuple


class Log:
    term: int
    command: Literal["ENQUEUE", "DEQUEUE"]
    args: Tuple[str, ...]
