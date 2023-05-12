import os
from threading import Lock


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
