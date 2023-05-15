from rpyc.utils.server import ThreadedServer
from program import ServerService, ServerConfig, Startup


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    print(f"Starting server on {hostname}:{port}")

    # Initialize
    startup = Startup()
    startup.initialize()

    # Start server
    server = ThreadedServer(ServerService, port=port)
    server.start()
