import rpyc
from service import ClientService


if __name__ == "__main__":
    conn = rpyc.connect("localhost", 8080, service=ClientService)

    print(conn.root.hello_world())
