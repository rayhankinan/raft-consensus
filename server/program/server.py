from threading import Lock
from rpyc.utils.server import ThreadedServer
from . import ServerConfig, ServerService


class ServerMeta(type):
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class Server(metaclass=ServerMeta):
    # Utility
    __config: ServerConfig = ServerConfig()

    # State
    __server = ThreadedServer(
        ServerService,
        port=__config.get("SERVER_ADDRESS").port,
    )

    def start(self) -> None:
        self.__server.start()

    def stop(self) -> None:
        self.__server.close()
