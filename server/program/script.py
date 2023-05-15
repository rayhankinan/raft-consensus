import rpyc
import asyncio
from threading import Lock
from . import RaftNode, ServerConfig, ServerService, dynamically_call_procedure, serialize


class ScriptMeta(type):
    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if cls not in cls.__instances:
                instance = super().__call__(*args, **kwargs)
                cls.__instances[cls] = instance
        return cls.__instances[cls]


class Script(metaclass=ScriptMeta):
    __node = RaftNode()
    __config = ServerConfig()

    def start(self) -> None:
        current_leader_address = self.__node.get_current_leader_address()

        if current_leader_address == self.__config.get("SERVER_ADDRESS"):
            self.__node.leader_startup()
            return

        hostname, port = current_leader_address
        conn = rpyc.connect(
            hostname,
            port,
            service=ServerService,
        )

        if type(conn) != rpyc.Connection:
            raise RuntimeError("Failed to connect to leader")

        current_server_address = (self.__config.get("SERVER_ADDRESS"), )
        asyncio.run(
            dynamically_call_procedure(
                conn,
                "add_server",
                serialize(current_server_address),
            )
        )

    def stop(self) -> None:
        # TODO: Tambahkan membership change untuk menghilangkan node diri sendiri pada leader
        pass
