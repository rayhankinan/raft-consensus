import os
from meta import ThreadSafeSingletonMeta


class ServerConfig(metaclass=ThreadSafeSingletonMeta):
    __conf = {
        "SERVER_HOSTNAME": os.getenv("SERVER_HOSTNAME", "localhost"),
        "SERVER_PORT": os.getenv("SERVER_PORT", "8080")
    }
    __setters = []

    def get(self, name: str) -> str:
        return ServerConfig.__conf[name]

    def set(self, name: str, value: str) -> None:
        if name not in ServerConfig.__setters:
            raise NameError("Name not accepted in set() method")

        ServerConfig.__conf[name] = value
