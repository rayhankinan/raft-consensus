import asyncio
from rpyc.utils.server import ThreadedServer
from program import RaftNode, ServerService, ServerConfig, Address


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address: Address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    print(f"Starting server on {hostname}:{port}")

    # Initialize Raft node
    raft_node = RaftNode()
    raft_node.initialize()

    # Start server
    server = ThreadedServer(ServerService, port=port)
    server.start()
