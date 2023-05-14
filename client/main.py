import rpyc
import asyncio
from program import ClientService, dynamically_call_procedure


if __name__ == "__main__":
    conn: rpyc.Connection = rpyc.connect(
        "localhost",
        8080,
        service=ClientService,
    )

    asyncio.run(dynamically_call_procedure(conn, "print_known_address"))
    asyncio.run(dynamically_call_procedure(conn, "print_logs"))

    conn.close()
