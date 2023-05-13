import rpyc
from typing import Callable


def dynamically_call_procedure(conn: rpyc.Connection, func_name: str, *args, **kwargs) -> object:
    if not hasattr(conn.root, func_name):
        raise RuntimeError(f"Function {func_name} not found in server")

    if not callable(getattr(conn.root, func_name)):
        raise RuntimeError(f"Function {func_name} is not callable")

    func: Callable[..., object] = getattr(conn.root, func_name)

    return func(*args, **kwargs)
