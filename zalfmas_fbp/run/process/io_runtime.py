from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any, cast

import capnp


def kj_exception_description(error: capnp.KjException) -> str:
    return str(getattr(error, "description", error))


async def wait_for_tasks_or_stop[T](
    tasks: Iterable[asyncio.Future[T]],
    stop_requested: asyncio.Event,
) -> tuple[set[asyncio.Future[T]], bool]:
    task_set = set(tasks)
    if not task_set:
        return set(), False

    stop_task = asyncio.create_task(stop_requested.wait())
    try:
        done, _pending = await asyncio.wait(
            {*task_set, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        done_tasks = {cast("asyncio.Future[T]", task) for task in done if task is not stop_task}
        return done_tasks, stop_task in done
    finally:
        if not stop_task.done():
            stop_task.cancel()


async def cancel_tasks(tasks: Iterable[asyncio.Future[Any]]) -> None:
    pending = list(tasks)
    for task in pending:
        _ = task.cancel()
    _ = await asyncio.gather(*pending, return_exceptions=True)
