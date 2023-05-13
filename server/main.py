from rpyc.utils.server import ThreadedServer
from service import ServerService
from config import ServerConfig
from raft import RaftNode


if __name__ == "__main__":
    raft_node = RaftNode()  # Panggil ngasal

    config = ServerConfig()
    port = int(config.get("SERVER_PORT"))

    server = ThreadedServer(ServerService, port=port)
    server.start()
