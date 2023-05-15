from program import ServerService, ServerConfig, Script, Server


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
    server = Server()
    server.start()

    # Stop sequence, when server.stop() is invoked
    script.stop()
