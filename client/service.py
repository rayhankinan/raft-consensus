import rpyc


@rpyc.service
class ClientService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection) -> None:
        self._conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()
