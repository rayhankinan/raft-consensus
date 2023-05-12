import rpyc


@rpyc.service
class ClientService(rpyc.VoidService):
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection):
        self._conn = conn

    def on_disconnect(self, _: rpyc.Connection):
        # TODO: Implementasikan connection clean up disini
        pass

    @rpyc.exposed
    def hello_mars(self) -> None:
        print("Hello Mars!")
