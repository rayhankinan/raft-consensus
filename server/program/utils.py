import rpyc
import asyncio
import codecs
import pickle
from typing import Callable, Any, Optional


async def dynamically_call_procedure(conn: rpyc.Connection, func_name: str, *args: bytes, **kwargs: bytes) -> Optional[bytes]:
    if not hasattr(conn.root, func_name):
        raise RuntimeError(f"Function {func_name} not found in server")

    if not callable(getattr(conn.root, func_name)):
        raise RuntimeError(f"Function {func_name} is not callable")

    func: Callable[..., Any] = getattr(conn.root, func_name)

    return await asyncio.to_thread(func, *args, **kwargs)


def serialize(value: Any) -> bytes:
    return codecs.encode(pickle.dumps(value), "base64")


def deserialize(value: bytes) -> Any:
    return pickle.loads(codecs.decode(value, "base64"))
