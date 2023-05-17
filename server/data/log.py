from typing import Literal, Tuple, NamedTuple
from . import Address


class StateLog(NamedTuple):
    term: int
    command: Literal["ENQUEUE", "DEQUEUE"]
    args: Tuple[str, ...]

    def get_term(self) -> int:
        return self.term

    def get_command(self) -> Literal["ENQUEUE", "DEQUEUE"]:
        return self.command

    def get_args(self) -> Tuple[str, ...]:
        return self.args


class MembershipLog(NamedTuple):
    term: int
    command: Literal["ADD_NODE", "REMOVE_NODE"]
    args: Tuple[Address, ...]

    def get_term(self) -> int:
        return self.term

    def get_command(self) -> Literal["ADD_NODE", "REMOVE_NODE"]:
        return self.command

    def get_args(self) -> Tuple[Address, ...]:
        return self.args
