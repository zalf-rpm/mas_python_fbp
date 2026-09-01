from __future__ import annotations

import asyncio
from typing import Any, cast

from mas.schema.common import common_capnp

from tests.component_harness import InMemoryReader, InMemoryWriter, done_message, ip_message
from zalfmas_fbp.components.simple.counter import METADATA, Counter

RPC_CONTEXT = cast("Any", None)


class _StopAfterNWriter(InMemoryWriter):
    """Writes normally, but requests a process stop right after the n-th write."""

    def __init__(self, component: Counter, n: int):
        super().__init__()
        self.component = component
        self.n = n
        self.calls = 0

    async def write(self, value: Any) -> None:
        self.calls += 1
        await super().write(value)
        if self.calls == self.n:
            self.component.context.lifecycle.stop_requested.set()


class _StepBlockingWriter(InMemoryWriter):
    """Writes normally, except the n-th write blocks until released.

    Used to deterministically race a reset against an in-flight write: the value for the
    blocked write is already fixed (it was built before write() was called), so it lets a
    test observe whether a reset landing during that write correctly affects only later
    writes instead of being silently overwritten by the post-write increment.
    """

    def __init__(self, release: asyncio.Event, block_at_call: int):
        super().__init__()
        self.release = release
        self.block_at_call = block_at_call
        self.calls = 0
        self.blocking_started = asyncio.Event()

    async def write(self, value: Any) -> None:
        self.calls += 1
        if self.calls == self.block_at_call:
            self.blocking_started.set()
            await self.release.wait()
        # yield on every call (like a real channel write would), so a test driving this
        # writer from another task always gets a chance to run in between writes
        await asyncio.sleep(0)
        await super().write(value)


def _counts(writer: InMemoryWriter) -> list[int]:
    return [v.content.as_struct(common_capnp.Value).i64 for v in writer.values]


async def _run_to_completion(component: Counter) -> None:
    assert await component.start(RPC_CONTEXT) is True
    lifecycle = component.context.lifecycle
    assert lifecycle.run_task is not None
    await lifecycle.run_task
    if lifecycle.run_exception is not None:
        raise lifecycle.run_exception


def test_counter_counts_up_from_start_at_without_reset_connected() -> None:
    async def run_test() -> None:
        component = Counter(METADATA)
        component.apply_config_values({"start_at": 5})
        writer = _StopAfterNWriter(component, n=3)
        component.out_ports["count"] = cast("Any", writer)

        await _run_to_completion(component)

        assert _counts(writer) == [5, 6, 7]

    asyncio.run(run_test())


def test_counter_defaults_start_at_to_zero() -> None:
    async def run_test() -> None:
        component = Counter(METADATA)
        writer = _StopAfterNWriter(component, n=2)
        component.out_ports["count"] = cast("Any", writer)

        await _run_to_completion(component)

        assert _counts(writer) == [0, 1]

    asyncio.run(run_test())


def test_counter_reset_takes_effect_and_survives_a_racing_in_flight_write() -> None:
    async def run_test() -> None:
        component = Counter(METADATA)
        component.apply_config_values({"start_at": 100})

        release = asyncio.Event()
        # write #2 (value=101) blocks; the reset is applied while it's in flight
        writer = _StepBlockingWriter(release, block_at_call=2)
        component.out_ports["count"] = cast("Any", writer)
        component.in_ports["reset"] = cast(
            "Any",
            InMemoryReader([ip_message("trigger"), done_message()]),
        )

        assert await component.start(RPC_CONTEXT) is True
        lifecycle = component.context.lifecycle
        assert lifecycle.run_task is not None

        await writer.blocking_started.wait()
        # the reset reader's single message is available immediately, so by now
        # watch_reset() has already run to completion and reset count to start_at
        release.set()

        # let a couple more (unblocked) writes happen, then stop
        while writer.calls < 4:
            await asyncio.sleep(0)
        lifecycle.stop_requested.set()

        await lifecycle.run_task
        if lifecycle.run_exception is not None:
            raise lifecycle.run_exception

        # write #2 (101) was already in flight when the reset landed, so it still
        # carries the pre-reset value; write #3 is the first one to reflect the reset
        assert _counts(writer) == [100, 101, 100, 101]

    asyncio.run(run_test())


def test_counter_reset_input_port_reaching_done_stops_watching_for_resets() -> None:
    async def run_test() -> None:
        component = Counter(METADATA)
        writer = _StopAfterNWriter(component, n=1)
        component.out_ports["count"] = cast("Any", writer)
        component.in_ports["reset"] = cast("Any", InMemoryReader([done_message()]))

        await _run_to_completion(component)

        assert component.in_ports["reset"] is None
        assert _counts(writer) == [0]

    asyncio.run(run_test())
