import rpyc
from typing import Tuple
from . import RaftNode, MembershipLog, Address, ServerConfig, Role, serialize, deserialize


@rpyc.service
class ServerService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __node: RaftNode
    __config: ServerConfig
    __conn: rpyc.Connection  # Two Way Communication

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__node = RaftNode()
        self.__config = ServerConfig()
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()

    # Function
    @rpyc.exposed
    def get_current_address(self) -> bytes:
        return serialize(self.__config.get("SERVER_ADDRESS"))

    # Procedure
    @rpyc.exposed
    def add_server(self, raw_follower_address: bytes) -> None:
        if self.__node.get_current_role() != Role.LEADER:
            raise RuntimeError("Not a leader")

        follower_addresses: Tuple[Address, ...] = deserialize(
            raw_follower_address
        )

        new_membership_log = MembershipLog(
            self.__node.get_current_term(),
            "ADD_NODE",
            follower_addresses
        )

        self.__node.add_membership_log(new_membership_log)
        # TODO: Broadcast add log to all nodes and wait for majority

        self.__node.commit_membership_log()
        # TODO: Broadcast commit log to all nodes and wait for majority

    # Procedure
    @rpyc.exposed
    def add_membership_log(self, membership_log: bytes) -> None:
        self.__node.add_membership_log(deserialize(membership_log))

    # Procedure
    @rpyc.exposed
    def commit_membership_log(self) -> None:
        self.__node.commit_membership_log()

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_membership_log(self) -> None:
        print("Logs:", self.__node.get_membership_log())

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        print("Known Address:", self.__node.get_current_known_address())
