import rpyc
import asyncio
from typing import Callable, Any


async def dynamically_call_procedure(conn: rpyc.Connection, func_name: str, *args, **kwargs) -> Any:
    if not hasattr(conn.root, func_name):
        raise RuntimeError(f"Function {func_name} not found in server")

    if not callable(getattr(conn.root, func_name)):
        raise RuntimeError(f"Function {func_name} is not callable")

    func: Callable[..., Any] = getattr(conn.root, func_name)

    return await asyncio.to_thread(func, *args, **kwargs)
