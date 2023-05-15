import rpyc
import asyncio
from typing import Tuple
from . import RaftNode, MembershipLog, Address, ServerConfig, Role, dynamically_call_procedure, serialize, deserialize


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
        # If not leader, forward to leader
        if self.__node.get_current_role() != Role.LEADER:
            current_leader_address = self.__node.get_current_leader_address()
            hostname, port = current_leader_address
            conn = rpyc.connect(
                hostname,
                port,
                service=ServerService,
            )

            if type(conn) != rpyc.Connection:
                raise RuntimeError("Failed to connect to leader")

            return asyncio.run(
                dynamically_call_procedure(
                    conn,
                    "add_server",
                    raw_follower_address,
                )
            )

        follower_addresses: Tuple[Address, ...] = deserialize(
            raw_follower_address
        )

        new_membership_log = MembershipLog(
            self.__node.get_current_term(),
            "ADD_NODE",
            follower_addresses
        )

        self.__node.add_membership_log(new_membership_log)

        # TODO: Broadcast append membership logs to all nodes and wait for majority

        self.__node.commit_membership_log()

        # TODO: Broadcast commit membership logs to all nodes and wait for majority

    # Procedure
    def append_membership_logs(self, raw_term: bytes, raw_membership_logs: bytes) -> None:
        term: int = deserialize(raw_term)

        if term < self.__node.get_current_term():
            raise RuntimeError("Term is too old")

        # TODO: Lanjutkan ini

        pass

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_membership_log(self) -> None:
        print("Membership Logs:", self.__node.get_membership_log())

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        print("Known Address:", self.__node.get_current_known_address())
