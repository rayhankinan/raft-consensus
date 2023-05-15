import rpyc
import asyncio
from . import RaftNode, Log, Address, ServerConfig, dynamically_call_procedure, serialize, deserialize


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __node: RaftNode
    __config: ServerConfig
    __conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__node = RaftNode()
        self.__config = ServerConfig()
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    @rpyc.exposed
    def apply_membership(self) -> None:
        follower_address: Address = deserialize(
            asyncio.run(
                dynamically_call_procedure(
                    self.__conn,
                    "get_current_address",
                )
            )
        )

        new_log = Log(
            self.__node.get_current_term(),
            "ADD_NODE",
            follower_address,
        )

        self.__node.add_log(new_log)
        # TODO: Broadcast add log to all nodes and wait for majority

        self.__node.commit_log()
        # TODO: Broadcast commit log to all nodes and wait for majority

    @rpyc.exposed
    def get_current_address(self) -> bytes:
        return serialize(self.__config.get("SERVER_ADDRESS"))

    # Test untuk client
    @rpyc.exposed
    def print_logs(self) -> None:
        print("Logs:", self.__node.get_logs())

    # Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        print("Known Address:", self.__node.get_current_known_address())
