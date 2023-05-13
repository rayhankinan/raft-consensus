import asyncio
from rpyc.utils.server import ThreadedServer
from service import RaftNode, ServerService
from config import ServerConfig
from address import Address


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address: Address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    print(f"Starting server on {hostname}:{port}")

    # Initialize Raft node
    raft_node = RaftNode()

    # Run all async methods
    asyncio.run(raft_node.initialize())

    # Start server
    server = ThreadedServer(ServerService, port=port)
    server.start()
