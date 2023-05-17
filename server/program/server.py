from threading import Thread, Lock
from typing import Callable, Optional
from rpyc.utils.server import ThreadedServer
from . import ServerConfig, ServerService, RaftNode


class ServerMeta(type):  # Thread Safe Singleton
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class Server(metaclass=ServerMeta):  # Ini Singleton
    # Utility
    __config = ServerConfig()
    __node = RaftNode()

    # State
    __server: ThreadedServer = ThreadedServer(
        ServerService,
        port=__config.get("SERVER_ADDRESS").get_port(),
    )

    def start(self, function: Optional[Callable[[], None]]) -> None:
        # Server Thread
        server_thread = Thread(target=self.__server.start)
        server_thread.start()

        # Start sequence
        self.__node.start()

        # Execute function if exists
        if function != None:
            function()

        # Join server thread
        server_thread.join()
