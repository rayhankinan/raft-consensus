from typing import NamedTuple
from . import Address


class ServerInfo(NamedTuple):
    address: Address
    next_index: int
    match_index: int
