import rpyc
from typing import Callable
from utils import dynamically_call_procedure


@rpyc.service
class ServerService(rpyc.Service):  # Stateful: Tidak menggunakan singleton
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection):
        self._conn = conn

    def on_disconnect(self, _: rpyc.Connection):
        # TODO: Implementasikan connection clean up disini
        pass

    @rpyc.exposed
    def hello_world(self) -> None:
        print("Hello World!")

        dynamically_call_procedure(self._conn, "hello_mars")
