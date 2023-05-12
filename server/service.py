import rpyc


@rpyc.service
class ServerService(rpyc.Service):
    @rpyc.exposed
    def hello_world(self) -> str:
        return "Hello World!"
