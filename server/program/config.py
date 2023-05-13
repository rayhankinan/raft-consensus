import os
from threading import Lock
from . import Address


class ServerConfigMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class ServerConfig(metaclass=ServerConfigMeta):
    __conf = {
        "SERVER_ADDRESS": Address(os.getenv("SERVER_HOSTNAME", "localhost"), int(os.getenv("SERVER_PORT", "8080"))),
        "LEADER_ADDRESS": Address(os.getenv("LEADER_HOSTNAME", "localhost"), int(os.getenv("LEADER_PORT", "8080"))),
        "TIMEOUT": int(os.getenv("TIMEOUT", "5")),
    }
    __setters = []

    def get(self, name: str) -> Address:
        return ServerConfig.__conf[name]

    def set(self, name: str, address: Address) -> None:
        if name not in ServerConfig.__setters:
            raise NameError("Name not accepted in set() method")

        ServerConfig.__conf[name] = address
