from typing import Literal, Tuple, NamedTuple
from . import Address


class StateLog(NamedTuple):
    term: int
    command: Literal["ENQUEUE", "DEQUEUE"]
    args: Tuple[str, ...]


class MembershipLog(NamedTuple):
    term: int
    command: Literal["ADD_NODE", "REMOVE_NODE"]
    args: Tuple[Address, ...]
