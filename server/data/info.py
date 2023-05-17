from typing import NamedTuple
from . import Address


class ServerInfo(NamedTuple):
    next_index: int
    match_index: int

    def get_next_index(self) -> int:
        return self.next_index

    def get_match_index(self) -> int:
        return self.match_index
