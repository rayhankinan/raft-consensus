import rpyc


@rpyc.service
class ClientService(rpyc.VoidService):  # Stateful: Tidak menggunakan singleton
    __conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection) -> None:
        self.__conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        conn.close()
