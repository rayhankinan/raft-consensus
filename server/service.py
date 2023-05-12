import rpyc


@rpyc.service
class ServerService(rpyc.Service):
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection):
        self._conn = conn

    def on_disconnect(self, _: rpyc.Connection):
        # TODO: Implementasikan connection clean up disini
        pass

    @rpyc.exposed
    def hello_world(self) -> str:
        return "Hello World!"
