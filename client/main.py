import rpyc
from typing import Callable
from service import ClientService


if __name__ == "__main__":
    conn: rpyc.Connection = rpyc.connect(
        "localhost",
        8080,
        service=ClientService
    )

    if hasattr(conn.root, "hello_world") and callable(getattr(conn.root, "hello_world")):
        func: Callable[[], None] = getattr(conn.root, "hello_world")
        func()
