from rpyc.utils.server import ThreadedServer
from service import ServerService
from config import ServerConfig
from raft import RaftNode


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    hostname = config.get("SERVER_HOSTNAME")
    port = int(config.get("SERVER_PORT"))

    print(f"Starting server on {hostname}:{port}")

    # Start Raft node
    raft_node = RaftNode()

    server = ThreadedServer(ServerService, port=port)
    server.start()
