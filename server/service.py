import rpyc
from typing import Callable
from raft import RaftNode


@rpyc.service
class ServerService(rpyc.Service):
    _conn: rpyc.Connection

    def on_connect(self, conn: rpyc.Connection):
        self._conn = conn

    def on_disconnect(self, _: rpyc.Connection):
        # TODO: Implementasikan connection clean up disini
        pass

    @rpyc.exposed
    def hello_world(self) -> None:
        raft_node = RaftNode()

        print("Hello World!")

        if hasattr(self._conn.root, "hello_mars") and callable(getattr(self._conn.root, "hello_mars")):
            func: Callable[[], None] = getattr(self._conn.root, "hello_mars")
            func()
