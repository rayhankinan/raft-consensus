import os
from threading import Lock
from data import Address


class ServerConfigMeta(type):
    # Utility
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class ServerConfig(metaclass=ServerConfigMeta):
    # Utility
    __conf = {
        "SERVER_ADDRESS": Address(os.getenv("SERVER_HOSTNAME", "localhost"), int(os.getenv("SERVER_PORT", "8080"))),
        "LEADER_ADDRESS": Address(os.getenv("LEADER_HOSTNAME", "localhost"), int(os.getenv("LEADER_PORT", "8080"))),
    }
    __setters = []

    def get(self, name: str) -> Address:
        return ServerConfig.__conf[name]

    def set(self, name: str, address: Address) -> None:
        if name not in ServerConfig.__setters:
            raise NameError("Name not accepted in set() method")

        ServerConfig.__conf[name] = address
