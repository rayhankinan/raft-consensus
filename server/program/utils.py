import rpyc
import asyncio
import codecs
import pickle
from typing import Callable, Optional, Coroutine, Any


async def dynamically_call_procedure(conn: rpyc.Connection, func_name: str, *args: bytes, **kwargs: bytes) -> Optional[bytes]:
    if not hasattr(conn.root, func_name):
        raise RuntimeError(f"Function {func_name} not found in server")

    if not callable(getattr(conn.root, func_name)):
        raise RuntimeError(f"Function {func_name} is not callable")

    func: Callable[..., Optional[bytes]] = getattr(conn.root, func_name)

    return await asyncio.to_thread(func, *args, **kwargs)


async def wait_for_all(*args: Coroutine[Any, Any, Optional[bytes]]) -> list[Optional[bytes]]:
    return await asyncio.gather(*args)


def serialize(value: Any) -> bytes:
    return codecs.encode(pickle.dumps(value), "base64")


def deserialize(value: bytes) -> Any:
    return pickle.loads(codecs.decode(value, "base64"))
