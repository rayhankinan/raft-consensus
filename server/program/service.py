import rpyc
import asyncio
from typing import Tuple
from data import MembershipLog, Address, Role
from . import RaftNode, ServerConfig, dynamically_call_procedure, deserialize


def create_connection(address: Address) -> rpyc.Connection:
    hostname, port = address
    conn = rpyc.connect(
        hostname,
        port,
        service=ServerService,
    )

    if type(conn) != rpyc.Connection:
        raise RuntimeError(f"Failed to connect to {hostname}:{port}")

    return conn


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

    # Procedure
    @rpyc.exposed
    def add_server(self, raw_follower_address: bytes) -> None:
        # Guard: if not leader, forward to leader
        if self.__node.get_current_role() != Role.LEADER:
            current_leader_address = self.__node.get_current_leader_address()
            conn = create_connection(current_leader_address)

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

        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        self.__node.add_server(follower_addresses)

    # TODO: Masih Untested
    # Procedure
    def append_membership_logs(self, raw_term: bytes, raw_prev_log_index: bytes, raw_prev_log_term: bytes, raw_new_membership_logs: bytes, raw_leader_commit_index: bytes) -> None:
        term: int = deserialize(raw_term)
        prev_log_index: int = deserialize(raw_prev_log_index)
        prev_log_term: int = deserialize(raw_prev_log_term)
        new_membership_logs: list[MembershipLog] = deserialize(
            raw_new_membership_logs
        )
        leader_commit_index: int = deserialize(raw_leader_commit_index)

        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        self.__node.append_membership_logs(
            term, prev_log_index,
            prev_log_term,
            new_membership_logs,
            leader_commit_index,
        )

    # TODO: Masih Untested
    # Procedure
    def commit_membership_logs(self) -> None:
        pass

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_membership_log(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        print("Membership Logs:", self.__node.get_membership_log())

    # Procedure: Test untuk client
    @rpyc.exposed
    def print_known_address(self) -> None:
        # NOTE: Dalam satu service hanya boleh terpanggil satu method pada node (menjaga atomicity)
        print("Known Address:", self.__node.get_current_known_address())
