from program import ServerConfig, Server


if __name__ == "__main__":
    # Get configuration
    config = ServerConfig()
    current_address = config.get("SERVER_ADDRESS")
    hostname, port = current_address

    # Start service
    server = Server()
    server.start(lambda: print(f"Server started at {hostname}:{port}"))
