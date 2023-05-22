import rpyc
import asyncio
from program import ClientService, dynamically_call_procedure, serialize


if __name__ == "__main__":
    conn_0 = rpyc.connect(
        "localhost",
        8080,
        service=ClientService,
    )
    conn_1 = rpyc.connect(
        "localhost",
        8081,
        service=ClientService,
    )
    conn_2 = rpyc.connect(
        "localhost",
        8082,
        service=ClientService,
    )
    conn_3 = rpyc.connect(
        "localhost",
        8083,
        service=ClientService,
    )
    conn_4 = rpyc.connect(
        "localhost",
        8084,
        service=ClientService,
    )
    conn_5 = rpyc.connect(
        "localhost",
        8085,
        service=ClientService,
    )

    if type(conn_0) != rpyc.Connection or type(conn_1) != rpyc.Connection or type(conn_2) != rpyc.Connection or type(conn_3) != rpyc.Connection or type(conn_4) != rpyc.Connection or type(conn_5) != rpyc.Connection or conn_0.closed or conn_1.closed or conn_2.closed or conn_3.closed or conn_4.closed or conn_5.closed:
        raise RuntimeError("Failed to connect to server")

    asyncio.run(
        dynamically_call_procedure(
            conn_5, "print_node"),
    )
