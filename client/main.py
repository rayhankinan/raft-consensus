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

    asyncio.run(
        dynamically_call_procedure(
            conn_4, "enqueue", serialize(("Testing!", ))
        ),
    )
