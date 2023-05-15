from rpyc.utils.server import ThreadedServer
from program import ServerService, ServerConfig, Script


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    # Start sequence
    script = Script()
    script.start()

    print(f"Starting server on {hostname}:{port}")

    # Start service
    server = ThreadedServer(ServerService, port=port)
    server.start()

    # Stop sequence
    script.stop()
