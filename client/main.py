import rpyc
import asyncio
from service import ClientService
from utils import dynamically_call_procedure


if __name__ == "__main__":
    conn: rpyc.Connection = rpyc.connect(
        "localhost",
        8080,
        service=ClientService
    )

    asyncio.run(dynamically_call_procedure(conn, "print_membership"))

    conn.close()
