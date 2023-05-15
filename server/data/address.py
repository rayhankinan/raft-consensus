from __future__ import annotations
from typing import NamedTuple


class Address(NamedTuple):
    hostname: str
    port: int
