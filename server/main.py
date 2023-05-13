from rpyc.utils.server import ThreadedServer
from service import ServerService
from config import ServerConfig
from raft import RaftNode
from address import Address


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address: Address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    print(f"Starting server on {hostname}:{port}")

    # Start Raft node
    raft_node = RaftNode()

    server = ThreadedServer(ServerService, port=port)
    server.start()
