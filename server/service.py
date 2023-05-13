import rpyc
from utils import dynamically_call_procedure


@rpyc.service
class ServerService(rpyc.Service):  # Stateful: Tidak menggunakan singleton
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection):
        self._conn = conn

    def on_disconnect(self, conn: rpyc.Connection):
        conn.close()

    @rpyc.exposed
    def hello_world(self) -> None:
        print("Hello World!")

        dynamically_call_procedure(self._conn, "hello_mars")
