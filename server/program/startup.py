import rpyc
import asyncio
from threading import Lock
from . import RaftNode, ServerConfig, ServerService, dynamically_call_procedure


class StartupMeta(type):
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class Startup(metaclass=StartupMeta):
    __node = RaftNode()
    __config = ServerConfig()

    def initialize(self) -> None:
        current_leader_address = self.__node.get_current_leader_address()

        if current_leader_address == self.__config.get("SERVER_ADDRESS"):
            self.__node.leader_startup()
        else:
            hostname, port = current_leader_address
            conn: rpyc.Connection = rpyc.connect(
                hostname,
                port,
                service=ServerService,
            )

            asyncio.run(
                dynamically_call_procedure(
                    conn,
                    "apply_membership",
                )
            )
