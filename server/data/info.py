from typing import NamedTuple
from . import Address


class ServerInfo(NamedTuple):
    next_index: int
    match_index: int
