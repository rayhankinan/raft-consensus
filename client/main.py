import rpyc
import asyncio
from program import ClientService, dynamically_call_procedure


if __name__ == "__main__":
    conn = rpyc.connect(
        "localhost",
        8085,
        service=ClientService,
    )

    if type(conn) != rpyc.Connection:
        raise RuntimeError("Failed to connect to server")

    asyncio.run(dynamically_call_procedure(conn, "print_known_address"))
    asyncio.run(dynamically_call_procedure(conn, "print_membership_log"))

    conn.close()
