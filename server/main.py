from rpyc.utils.server import ThreadedServer
from service import ServerService
from config import ServerConfig


if __name__ == "__main__":
    config = ServerConfig()
    port = int(config.get("SERVER_PORT"))

    server = ThreadedServer(ServerService, port=port)
    server.start()
