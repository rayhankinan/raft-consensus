from typing import NamedTuple


class Address(NamedTuple):
    hostname: str
    port: int

    def get_hostname(self) -> str:
        return self.hostname

    def get_port(self) -> int:
        return self.port
