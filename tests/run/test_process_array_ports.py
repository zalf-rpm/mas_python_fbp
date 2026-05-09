from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    InMemoryReader,
    InMemoryWriter,
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    text_outputs,
)
from zalfmas_fbp.run import process
from zalfmas_fbp.run.metadata import ComponentMetadata

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.enums import ProcessStateEnum


class _FakeCap:
    def __init__(self, port: object):
        self.port = port

    def cast_as(self, _schema: object) -> object:
        return self.port


class _FakeConnectionManager:
    def __init__(self, port: object | None):
        self.port = port

    async def try_connect(self, _sturdy_ref: object) -> _FakeCap | None:
        if self.port is None:
            return None
        return _FakeCap(self.port)


class _ClosablePort:
    def __init__(self):
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _DelayedReader(InMemoryReader):
    def __init__(self, messages: list[Any], delay: float):
        super().__init__(messages)
        self.delay = delay

    async def read(self):
        await asyncio.sleep(self.delay)
        return await super().read()


class _BrokenReader:
    async def read(self):
        raise capnp.KjException("channel closed")


class _BlockingBrokenReader:
    def __init__(self):
        self.started = asyncio.Event()

    async def read(self):
        self.started.set()
        await asyncio.Future()


class _BlockingWriter(InMemoryWriter):
    def __init__(self, release: asyncio.Event):
        super().__init__()
        self.release = release
        self.started = asyncio.Event()

    async def write(self, value: Any) -> None:
        self.started.set()
        await self.release.wait()
        await super().write(value)


def test_default_bracketed_chunk_size_is_16_mib() -> None:
    assert process.DEFAULT_BRACKETED_CHUNK_SIZE == 16 * 1024 * 1024


class _SignalingWriter(InMemoryWriter):
    def __init__(self):
        super().__init__()
        self.written = asyncio.Event()

    async def write(self, value: Any) -> None:
        await super().write(value)
        self.written.set()


class _FailOnceThenWriteWriter(InMemoryWriter):
    def __init__(self):
        super().__init__()
        self.calls = 0

    async def write(self, value: Any) -> None:
        self.calls += 1
        if self.calls == 2:
            raise capnp.KjException("temporary mid-stream failure")
        await super().write(value)


class _StopAwareProcess(process.Process):
    async def run(self) -> None:
        while not self.stopping:
            await asyncio.sleep(0)


class _FailOnceProcess(process.Process):
    def __init__(self):
        super().__init__()
        self.runs = 0

    async def run(self) -> None:
        self.runs += 1
        if self.runs == 1:
            raise RuntimeError("expected test failure")
        while not self.stopping:
            await asyncio.sleep(0)


class _CancellationSuppressingProcess(process.Process):
    def __init__(self):
        super().__init__()
        self.release = asyncio.Event()
        self.cancel_count = 0

    async def run(self) -> None:
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancel_count += 1


class _ReadOnceProcess(process.Process):
    async def run(self) -> None:
        await self.read_in("in")


class _CatchesReadErrorProcess(process.Process):
    def __init__(self):
        super().__init__(metadata=_standard_port_meta())
        self.caught_error = False

    async def run(self) -> None:
        try:
            await self.read_in("in")
        except process.InputPortReadError:
            self.caught_error = True


def _always_fail(*_args: str) -> bool:
    raise RuntimeError("boom")


def _raise_multiline_runtime_error() -> None:
    if not _always_fail(
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
    ):
        return


def test_process_metadata_initializes_array_in_and_out_ports() -> None:
    component = process.Process(metadata=_array_port_meta())

    assert component.process_state == "idle"
    assert "items" in component.array_in_ports
    assert "items" not in component.in_ports
    assert "out" in component.array_out_ports
    assert "out" not in component.out_ports

    in_ports = asyncio.run(component.inPorts(cast("Any", None)))
    out_ports = asyncio.run(component.outPorts(cast("Any", None)))

    assert {"name": "items", "type": "array", "contentType": "Text"} in in_ports
    assert {"name": "out", "type": "array", "contentType": "Text"} in out_ports


def test_process_soft_stop_returns_to_idle_and_clears_stopping_flag() -> None:
    async def run_test() -> None:
        component = _StopAwareProcess()

        assert component.process_state == "idle"
        assert await component.start() is True
        await _wait_for_state(component, "running")

        assert await component.stop() is True

        assert component.process_state == "idle"
        assert component.stopping is False

    asyncio.run(run_test())


def test_process_stop_timeout_returns_false_while_task_is_still_stopping() -> None:
    async def run_test() -> None:
        component = _CancellationSuppressingProcess()
        component.soft_stop_timeout_seconds = 0.01

        assert await component.start() is True
        await _wait_for_state(component, "running")

        assert await component.stop() is False

        assert component.process_state == "stopping"
        assert component.stopping is True

        component.release.set()
        await _wait_for_state(component, "idle")
        assert component.stopping is False

    asyncio.run(run_test())


def test_unexpected_input_port_failure_fails_process_and_records_last_error() -> None:
    async def run_test() -> None:
        component = _ReadOnceProcess(metadata=_standard_port_meta())
        component.in_ports["in"] = cast("Any", _BrokenReader())

        assert await component.start() is True
        await _wait_for_state(component, "failed")

        error_info = await component.lastError()
        assert error_info.hasError is True
        assert error_info.phase == "read"
        assert error_info.port == "in"
        assert error_info.errorType == "InputPortReadError"
        assert error_info.causeType == "KjException"

    asyncio.run(run_test())


def test_component_can_catch_input_port_failure_inside_run() -> None:
    async def run_test() -> None:
        component = _CatchesReadErrorProcess()
        component.in_ports["in"] = cast("Any", _BrokenReader())

        assert await component.start() is True
        if component._run_task is None:
            raise AssertionError("Process did not create a run task.")
        await component._run_task

        assert component.caught_error is True
        assert component.process_state == "idle"
        error_info = await component.lastError()
        assert error_info.hasError is False
        assert error_info.phase == "unknown"

    asyncio.run(run_test())


def test_stop_requested_read_returns_none_without_bubbling_error() -> None:
    async def run_test() -> None:
        component = process.Process(metadata=_standard_port_meta())
        reader = _BlockingBrokenReader()
        component.in_ports["in"] = cast("Any", reader)

        read_task = asyncio.create_task(component.read_in("in"))
        await reader.started.wait()
        component._stop_requested.set()

        assert await asyncio.wait_for(read_task, timeout=1) is None

    asyncio.run(run_test())


def test_failed_process_can_be_restarted_and_stopped() -> None:
    async def run_test() -> None:
        component = _FailOnceProcess()

        assert await component.start() is True
        await _wait_for_state(component, "failed")
        assert component.runs == 1

        assert await component.start() is True
        await _wait_for_state(component, "running")
        assert component.runs == 2

        assert await component.stop() is True
        assert component.process_state == "idle"

    asyncio.run(run_test())


def test_write_array_out_broadcast_writes_to_all_connected_ports() -> None:
    component = process.Process(metadata=_array_port_meta())
    first = InMemoryWriter()
    second = InMemoryWriter()
    component.array_out_ports["out"] = [cast("Any", first), None, cast("Any", second)]

    wrote = asyncio.run(component.write_array_out("out", "broadcast", _text_ip("alpha")))

    assert wrote is True
    assert text_outputs(first) == ["alpha"]
    assert text_outputs(second) == ["alpha"]


def test_write_array_out_broadcast_does_not_block_other_ports_behind_a_slow_writer() -> None:
    async def run_test() -> None:
        component = process.Process(metadata=_array_port_meta())
        release = asyncio.Event()
        blocking = _BlockingWriter(release)
        ready = _SignalingWriter()
        component.array_out_ports["out"] = [cast("Any", blocking), cast("Any", ready)]

        write_task = asyncio.create_task(component.write_array_out("out", "broadcast", _text_ip("alpha")))

        await blocking.started.wait()
        await asyncio.wait_for(ready.written.wait(), timeout=0.1)
        assert text_outputs(ready) == ["alpha"]
        assert text_outputs(blocking) == []

        release.set()
        assert await write_task is True
        assert text_outputs(blocking) == ["alpha"]

    asyncio.run(run_test())


def test_write_array_out_next_available_distributes_across_idle_ports() -> None:
    async def run_test() -> None:
        component = process.Process(metadata=_array_port_meta())
        first = InMemoryWriter()
        second = InMemoryWriter()
        component.array_out_ports["out"] = [cast("Any", first), cast("Any", second)]

        assert (
            await component.write_array_out("out", process.ArrayOutStrategy.NEXT_AVAILABLE, _text_ip("alpha")) is True
        )
        assert await component.write_array_out("out", process.ArrayOutStrategy.NEXT_AVAILABLE, _text_ip("beta")) is True

        await component.close_out_ports()

        assert text_outputs(first) == ["alpha"]
        assert text_outputs(second) == ["beta"]

    asyncio.run(run_test())


def test_write_array_out_next_available_uses_other_idle_port_while_one_write_is_blocked() -> None:
    async def run_test() -> None:
        component = process.Process(metadata=_array_port_meta())
        release = asyncio.Event()
        blocking = _BlockingWriter(release)
        ready = _SignalingWriter()
        component.array_out_ports["out"] = [cast("Any", blocking), cast("Any", ready)]

        assert (
            await component.write_array_out("out", process.ArrayOutStrategy.NEXT_AVAILABLE, _text_ip("alpha")) is True
        )
        await blocking.started.wait()

        assert await component.write_array_out("out", process.ArrayOutStrategy.NEXT_AVAILABLE, _text_ip("beta")) is True
        await asyncio.wait_for(ready.written.wait(), timeout=0.1)
        assert text_outputs(ready) == ["beta"]
        assert text_outputs(blocking) == []

        release.set()
        await component.close_out_ports()
        assert text_outputs(blocking) == ["alpha"]

    asyncio.run(run_test())


def test_write_array_out_round_robin_uses_next_port() -> None:
    component = process.Process(metadata=_array_port_meta())
    first = InMemoryWriter()
    second = InMemoryWriter()
    component.array_out_ports["out"] = [cast("Any", first), cast("Any", second)]

    assert asyncio.run(component.write_array_out("out", "round_robin", _text_ip("alpha"))) is True
    assert asyncio.run(component.write_array_out("out", "round_robin", _text_ip("beta"))) is True
    component.array_out_ports["out"][0] = None
    assert asyncio.run(component.write_array_out("out", "round_robin", _text_ip("gamma"))) is True

    assert text_outputs(first) == ["alpha"]
    assert text_outputs(second) == ["beta", "gamma"]


def test_write_array_out_rejects_invalid_strategy() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_out_ports["out"] = [cast("Any", InMemoryWriter())]

    try:
        asyncio.run(component.write_array_out("out", "least_loaded", _text_ip("alpha")))
    except ValueError:
        pass
    else:
        raise AssertionError("least_loaded is not an array output strategy")


def test_read_array_in_zip_returns_one_message_from_every_active_port() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_in_ports["items"] = [
        cast("Any", InMemoryReader([ip_message("alpha"), done_message()])),
        cast("Any", InMemoryReader([ip_message("beta"), done_message()])),
    ]

    messages = asyncio.run(component.read_array_in("items", "zip"))

    assert messages is not None
    assert [msg.content.as_text() for msg in messages] == ["alpha", "beta"]


def test_read_array_in_zip_returns_none_when_any_port_is_done() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_in_ports["items"] = [
        cast("Any", InMemoryReader([done_message()])),
        cast("Any", InMemoryReader([done_message()])),
    ]

    assert asyncio.run(component.read_array_in("items", "zip")) is None
    assert component.array_in_ports["items"] == [None, None]


def test_read_array_in_next_available_returns_first_available_message() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_in_ports["items"] = [
        cast("Any", _DelayedReader([ip_message("slow"), done_message()], delay=0.05)),
        cast("Any", _DelayedReader([ip_message("fast"), done_message()], delay=0)),
    ]

    message = asyncio.run(component.read_array_in("items", process.ArrayInStrategy.NEXT_AVAILABLE))

    assert message is not None
    assert message.content.as_text() == "fast"


def test_read_array_in_next_available_skips_done_ports() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_in_ports["items"] = [
        cast("Any", InMemoryReader([done_message()])),
        cast("Any", InMemoryReader([ip_message("alpha"), done_message()])),
    ]

    message = asyncio.run(component.read_array_in("items", process.ArrayInStrategy.NEXT_AVAILABLE))

    assert message is not None
    assert message.content.as_text() == "alpha"
    assert component.array_in_ports["items"][0] is None


def test_read_array_in_next_available_bracketed_coalesces_chunks() -> None:
    component = process.Process(metadata=_array_port_meta())
    open_ip = fbp_capnp.IP.new_message(type="openBracket")
    close_ip = fbp_capnp.IP.new_message(type="closeBracket")
    component.array_in_ports["items"] = [
        cast(
            "Any",
            InMemoryReader(
                [
                    PortMessage(PortValue(open_ip)),
                    PortMessage(PortValue(_data_ip(b"pa"))),
                    PortMessage(PortValue(_data_ip(b"yload"))),
                    PortMessage(PortValue(close_ip)),
                    done_message(),
                ],
            ),
        ),
    ]

    message = asyncio.run(component.read_array_in("items", process.ArrayInStrategy.NEXT_AVAILABLE, True))

    assert message is not None
    assert bytes(message.content.as_struct(common_capnp.Value).d) == b"payload"


def test_read_in_rejects_bracketed_payload_when_disabled() -> None:
    component = process.Process(metadata=_standard_port_meta())
    open_ip = fbp_capnp.IP.new_message(type="openBracket")
    component.in_ports["in"] = cast("Any", InMemoryReader([PortMessage(PortValue(open_ip)), done_message()]))

    try:
        asyncio.run(component.read_in("in"))
    except process.InputPortReadError as exc:
        assert "bracketed reading is disabled" in str(exc)
    else:
        raise AssertionError("read_in should reject bracketed payloads unless bracketed is enabled")


def test_read_in_bracketed_coalesces_chunks() -> None:
    component = process.Process(metadata=_standard_port_meta())
    open_ip = fbp_capnp.IP.new_message(type="openBracket")
    first = _data_ip(b"pa")
    second = _data_ip(b"yload")
    close_ip = fbp_capnp.IP.new_message(type="closeBracket")
    component.in_ports["in"] = cast(
        "Any",
        InMemoryReader(
            [
                PortMessage(PortValue(open_ip)),
                PortMessage(PortValue(first)),
                PortMessage(PortValue(second)),
                PortMessage(PortValue(close_ip)),
                done_message(),
            ],
        ),
    )

    message = asyncio.run(component.read_in("in", True))

    assert message is not None
    assert bytes(message.content.as_struct(common_capnp.Value).d) == b"payload"
def test_write_out_bracketed_chunks_data(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    component = process.Process(metadata=_standard_port_meta())
    writer = InMemoryWriter()
    component.out_ports["out"] = cast("Any", writer)

    assert asyncio.run(component.write_out("out", _data_ip(b"payload"), True)) is True

    assert [ip.type for ip in writer.values] == [
        "openBracket",
        "standard",
        "standard",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert b"".join(bytes(ip.content.as_struct(common_capnp.Value).d) for ip in writer.values[1:-1]) == b"payload"


def test_write_out_bracketed_best_effort_closes_stream_after_midstream_failure(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    component = process.Process(metadata=_standard_port_meta())
    writer = _FailOnceThenWriteWriter()
    component.out_ports["out"] = cast("Any", writer)

    try:
        asyncio.run(component.write_out("out", _data_ip(b"payload"), True))
    except process.OutputPortWriteError as exc:
        assert "temporary mid-stream failure" in str(exc)
    else:
        raise AssertionError("write_out should fail when a chunk write raises KjException")

    assert [ip.type for ip in writer.values] == ["openBracket", "closeBracket"]
    assert component.out_ports["out"] is None


def test_record_error_keeps_full_multiline_traceback() -> None:
    component = process.Process(metadata=_standard_port_meta())

    try:
        _raise_multiline_runtime_error()
    except RuntimeError as exc:
        component._record_error(exc)
    else:
        raise AssertionError("multiline runtime error should have been raised")

    assert component._last_error is not None
    formatted_traceback = "".join(component._last_error.traceback or [])
    assert "if not _always_fail(" in formatted_traceback
    assert '"a",' in formatted_traceback
    assert '"h",' in formatted_traceback
    assert "...<" not in formatted_traceback


def test_read_in_returns_none_when_port_is_unconnected() -> None:
    component = process.Process(metadata=_standard_port_meta())

    assert asyncio.run(component.read_in("in")) is None


def test_read_array_in_returns_none_when_no_active_ports_are_connected() -> None:
    component = process.Process(metadata=_array_port_meta())

    assert asyncio.run(component.read_array_in("items", "zip")) is None


def test_connect_in_port_returns_disconnect_callback_for_standard_port() -> None:
    reader = _ClosablePort()
    component = process.Process(metadata=_standard_port_meta(), con_man=cast("Any", _FakeConnectionManager(reader)))

    connected, disconnect = asyncio.run(component.connectInPort("in", cast("Any", "reader-sr"), cast("Any", None)))
    disconnect = cast("process.PortDisconnect", disconnect)

    assert connected is True
    assert component.in_ports["in"] is reader
    assert asyncio.run(disconnect.disconnect()) is True
    assert component.in_ports["in"] is None
    assert reader.closed is True
    assert asyncio.run(disconnect.disconnect()) is False


def test_connect_in_port_returns_disconnect_callback_for_array_port() -> None:
    reader = _ClosablePort()
    component = process.Process(metadata=_array_port_meta(), con_man=cast("Any", _FakeConnectionManager(reader)))

    connected, disconnect = asyncio.run(component.connectInPort("items", cast("Any", "reader-sr"), cast("Any", None)))
    disconnect = cast("process.PortDisconnect", disconnect)

    assert connected is True
    assert component.array_in_ports["items"] == [reader]
    assert asyncio.run(disconnect.disconnect()) is True
    assert component.array_in_ports["items"] == [None]
    assert reader.closed is True
    assert asyncio.run(disconnect.disconnect()) is False


def test_connect_out_port_returns_disconnect_callback_for_standard_port() -> None:
    writer = _ClosablePort()
    component = process.Process(metadata=_standard_port_meta(), con_man=cast("Any", _FakeConnectionManager(writer)))

    connected, disconnect = asyncio.run(component.connectOutPort("out", cast("Any", "writer-sr"), cast("Any", None)))
    disconnect = cast("process.PortDisconnect", disconnect)

    assert connected is True
    assert component.out_ports["out"] is writer
    assert asyncio.run(disconnect.disconnect()) is True
    assert component.out_ports["out"] is None
    assert writer.closed is True
    assert asyncio.run(disconnect.disconnect()) is False


def test_connect_out_port_returns_disconnect_callback_for_array_port() -> None:
    writer = _ClosablePort()
    component = process.Process(metadata=_array_port_meta(), con_man=cast("Any", _FakeConnectionManager(writer)))

    connected, disconnect = asyncio.run(component.connectOutPort("out", cast("Any", "writer-sr"), cast("Any", None)))
    disconnect = cast("process.PortDisconnect", disconnect)

    assert connected is True
    assert component.array_out_ports["out"] == [writer]
    assert asyncio.run(disconnect.disconnect()) is True
    assert component.array_out_ports["out"] == [None]
    assert writer.closed is True
    assert asyncio.run(disconnect.disconnect()) is False


def test_disconnect_callback_for_failed_connection_is_noop() -> None:
    component = process.Process(metadata=_array_port_meta(), con_man=cast("Any", _FakeConnectionManager(None)))

    connected, disconnect = asyncio.run(component.connectOutPort("out", cast("Any", "writer-sr"), cast("Any", None)))
    disconnect = cast("process.PortDisconnect", disconnect)

    assert connected is False
    assert component.array_out_ports["out"] == [None]
    assert asyncio.run(disconnect.disconnect()) is False


def _text_ip(value: str) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=value)


def _data_ip(value: bytes) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=value))


async def _wait_for_state(component: process.Process, expected: ProcessStateEnum) -> None:
    async def wait() -> None:
        while component.process_state != expected:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=1)


def _standard_port_meta() -> ComponentMetadata:
    return ComponentMetadata.model_validate(
        {
            "info": {
                "id": "standard-port-test",
                "name": "standard-port-test",
                "description": "standard port test process",
            },
            "type": "process",
            "inPorts": [{"name": "in", "contentType": "Text"}],
            "outPorts": [{"name": "out", "contentType": "Text"}],
        },
    )


def _array_port_meta() -> ComponentMetadata:
    return ComponentMetadata.model_validate(
        {
            "info": {
                "id": "array-port-test",
                "name": "array-port-test",
                "description": "array port test process",
            },
            "type": "process",
            "inPorts": [{"name": "items", "type": "array", "contentType": "Text"}],
            "outPorts": [{"name": "out", "type": "array", "contentType": "Text"}],
        },
    )
