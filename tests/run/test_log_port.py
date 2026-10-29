"""The runtime-owned log port: a lossy tee that can never stall the flow (plan section 6.2)."""

from __future__ import annotations

import asyncio
import logging
from collections import deque

import pytest

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.process.runtime.log_runtime import level_name_for


class Config(process.ProcessConfig):
    pass


METADATA = meta.Component(
    info=meta.Info(id="7a1a2b3c-0000-4000-8000-00000000106f", name="log probe"),
    type="process",
    inPorts=[meta.Port(name="in")],
    outPorts=[meta.Port(name="out")],
    config=Config,
)


class NoSpaceWriter:
    """A channel whose buffer is always full."""

    def __init__(self):
        self.attempts = 0

    async def writeIfSpace(self, value):  # noqa: N802 - the capnp method name
        self.attempts += 1
        return type("R", (), {"success": False})()


class AcceptingWriter:
    def __init__(self):
        self.values = []

    async def writeIfSpace(self, value):  # noqa: N802 - the capnp method name
        self.values.append(value)
        return type("R", (), {"success": True})()


class BlockingWriter:
    """Has only the blocking write, as a channel predating writeIfSpace would."""

    def __init__(self):
        self.writes = 0

    async def write(self, value):
        self.writes += 1

    async def writeIfSpace(self, value):  # noqa: N802 - the capnp method name
        import capnp

        msg = "unimplemented"
        raise capnp.KjException(msg)


def make_component(writer=None):
    component = process.Process[Config](METADATA)
    if writer is not None:
        component.out_ports["log"] = writer
    return component


def record(level=logging.INFO, message="hello", name="test.logger", exc_info=None):
    return logging.LogRecord(name, level, "f.py", 1, message, None, exc_info)


# --- level mapping ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("levelno", "expected"),
    [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.WARNING, "warning"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "critical"),
        (logging.CRITICAL + 10, "critical"),
        (1, "debug"),
        (25, "info"),
    ],
)
def test_python_levels_map_onto_the_schema_enum(levelno, expected) -> None:
    assert level_name_for(levelno) == expected


# --- record conversion -----------------------------------------------------------------------


def test_a_record_becomes_a_log_message_carrying_the_process_identity() -> None:
    component = make_component()
    component.name = "probe-1"
    built = component._log_tee.message_for(record(message="something happened")).as_reader()

    assert built.message == "something happened"
    assert str(built.level) == "info"
    assert built.processName == "probe-1"
    assert built.logger == "test.logger"
    assert built.timestamp.startswith("20")


def test_long_messages_are_truncated() -> None:
    component = make_component()
    built = component._log_tee.message_for(record(message="x" * 9000)).as_reader()
    assert built.message.endswith("chars)")
    assert len(built.message) < 9000


def test_an_exception_is_carried_as_a_traceback() -> None:
    component = make_component()

    def boom():
        msg = "boom"
        raise ValueError(msg)

    try:
        boom()
    except ValueError as exc:
        built = component._log_tee.message_for(
            record(level=logging.ERROR, message="failed", exc_info=(type(exc), exc, exc.__traceback__)),
        ).as_reader()

    assert str(built.level) == "error"
    assert any("ValueError" in line for line in built.traceback)


# --- lossiness -------------------------------------------------------------------------------


def test_a_full_queue_drops_the_oldest_record_rather_than_blocking() -> None:
    """Bounded by construction: emit() can never block the component that logged."""
    component = make_component()
    tee = component._log_tee
    tee._queue = deque(maxlen=2)
    handler = logging.Handler()

    from zalfmas_fbp.run.process.runtime.log_runtime import _QueueHandler

    handler = _QueueHandler(tee._queue, tee._note_queue_drop)
    for i in range(5):
        handler.emit(record(message=str(i)))

    assert [r.getMessage() for r in tee._queue] == ["3", "4"]
    assert tee.dropped_full_queue == 3


def test_a_full_channel_drops_the_record_rather_than_blocking() -> None:
    writer = NoSpaceWriter()
    component = make_component(writer)
    tee = component._log_tee
    tee._queue.append(record(message="dropped"))

    asyncio.run(_drain_once(tee))

    assert writer.attempts == 1
    assert tee.dropped_full_channel == 1
    assert tee.written == 0


def test_records_reach_a_channel_with_room() -> None:
    writer = AcceptingWriter()
    component = make_component(writer)
    tee = component._log_tee
    tee._queue.append(record(message="delivered"))

    asyncio.run(_drain_once(tee))

    assert tee.written == 1
    assert len(writer.values) == 1


def test_a_channel_without_write_if_space_drops_rather_than_falling_back_to_blocking() -> None:
    """Dropping is the right answer: this path exists so it can never block."""
    writer = BlockingWriter()
    component = make_component(writer)
    tee = component._log_tee
    tee._queue.append(record(message="x"))

    asyncio.run(_drain_once(tee))

    assert writer.writes == 0
    assert tee.dropped_full_channel == 1


async def _drain_once(tee) -> None:
    task = asyncio.create_task(tee._drain())
    for _ in range(20):
        await asyncio.sleep(0)
        if not tee._queue:
            break
    _ = task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --- tee semantics ---------------------------------------------------------------------------


def test_the_tee_does_not_start_when_the_log_port_is_unconnected() -> None:
    component = make_component()
    before = len(logging.getLogger().handlers)
    component._log_tee.start()

    assert component._log_tee._task is None
    assert len(logging.getLogger().handlers) == before


def test_the_local_logger_keeps_its_own_handlers_while_the_tee_runs(caplog) -> None:
    """A tee, not a replacement: records must still reach the ordinary logger."""
    component = make_component(AcceptingWriter())
    root_handlers_before = list(logging.getLogger().handlers)

    async def scenario():
        component._log_tee.start()
        logging.getLogger("some.component").warning("still visible")
        await component._log_tee.close()

    with caplog.at_level(logging.WARNING):
        asyncio.run(scenario())

    assert "still visible" in caplog.text
    assert logging.getLogger().handlers == root_handlers_before


def test_the_log_port_is_declared_by_the_runtime_not_the_component() -> None:
    assert "log" not in [p.name for p in [meta.Port(name="out")]]
    assert [p.name for p in METADATA.outPorts] == ["out", "log"]
    assert METADATA.outPorts[-1].role == "log"
