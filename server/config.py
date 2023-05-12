import os


class ServerConfig:
    __conf = {
        "HOSTNAME": os.getenv("SERVER_HOSTNAME", "localhost"),
        "PORT": os.getenv("SERVER_PORT", "8080")
    }
    __setters = []

    @staticmethod
    def config(name: str) -> str:
        return ServerConfig.__conf[name]

    @staticmethod
    def set(name: str, value: str) -> None:
        if name not in ServerConfig.__setters:
            raise NameError("Name not accepted in set() method")

        ServerConfig.__conf[name] = value
