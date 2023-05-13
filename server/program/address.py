from __future__ import annotations
from typing import NamedTuple


class Address(NamedTuple):
    hostname: str
    port: int

    def __eq__(self, value: Address) -> bool:
        return self.hostname == value.hostname and self.port == value.port
