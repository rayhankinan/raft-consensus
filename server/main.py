from rpyc.utils.server import ThreadedServer
from service import ServerService
from config import ServerConfig


if __name__ == "__main__":
    port = int(ServerConfig.config("SERVER_PORT"))
    server = ThreadedServer(ServerService, port=port)

    server.start()
