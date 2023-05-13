import rpyc
from service import ClientService
from utils import dynamically_call_procedure


if __name__ == "__main__":
    conn: rpyc.Connection = rpyc.connect(
        "localhost",
        8080,
        service=ClientService
    )

    dynamically_call_procedure(conn, "hello_world")
