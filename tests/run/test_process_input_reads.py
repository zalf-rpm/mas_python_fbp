from __future__ import annotations

import asyncio
import contextlib

from tests.component_harness import (
    InFlightReader,
    InMemoryWriter,
    done_message,
    ip_message,
    text_outputs,
)
from zalfmas_fbp.run import process
from zalfmas_fbp.run.metadata import ComponentMetadata


def _port_meta() -> ComponentMetadata:
    return ComponentMetadata.model_validate(
        {
            "info": {
                "id": "input-read-test",
                "name": "input-read-test",
                "description": "input read test process",
            },
            "type": "process",
            "inPorts": [{"name": "in", "contentType": "Text"}],
            "outPorts": [{"name": "out", "contentType": "Text"}],
        },
    )


class _CancelsItsOwnReadProcess(process.Process):
    """Starts a read, gives up on waiting for it, and then reads again.

    This is what every component doing asyncio.wait(..., FIRST_COMPLETED) over several ports ends
    up doing, and what the substream assembly does at the end of each substream.
    """

    def __init__(self):
        super().__init__(metadata=_port_meta())

    async def run(self) -> None:
        read = asyncio.ensure_future(self.read_in("in"))
        await asyncio.sleep(0)  # the read starts and takes the IP out of the channel
        _ = read.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await read

        # the IP the abandoned read was carrying must not be lost
        ip = await self.read_in("in")
        if ip is not None:
            _ = await self.write_out("out", ip)


def test_read_abandoned_by_its_caller_is_taken_over_by_the_next_read() -> None:
    component = _CancelsItsOwnReadProcess()
    writer = InMemoryWriter()
    component.in_ports["in"] = InFlightReader([ip_message("a"), done_message()])  # pyright: ignore[reportAttributeAccessIssue]
    component.out_ports["out"] = writer  # pyright: ignore[reportAttributeAccessIssue]

    asyncio.run(_run(component))

    assert text_outputs(writer) == ["a"]


async def _run(component: process.Process) -> None:
    await component.start(None)  # pyright: ignore[reportArgumentType]
    run_task = component.context.lifecycle.run_task
    assert run_task is not None
    await run_task
    if component.context.lifecycle.run_exception is not None:
        raise component.context.lifecycle.run_exception
