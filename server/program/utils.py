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


async def wait_for_majority(*args: Coroutine[Any, Any, Optional[bytes]]) -> list[Optional[bytes]]:
    length = len(args)
    threshold = length // 2 + 1

    completed = 0
    results: list[Optional[bytes]] = []

    # Membuat list of task
    tasks = [asyncio.create_task(arg) for arg in args]

    while completed < threshold:
        # Menunggu salah satu task selesai
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            results.append(task.result())
            completed += 1

        # Hilangkan task yang sudah selesai

        tasks = [task for task in tasks if not task.done()]

    return results


def serialize(value: Any) -> bytes:
    return codecs.encode(pickle.dumps(value), "base64")


def deserialize(value: bytes) -> Any:
    return pickle.loads(codecs.decode(value, "base64"))
