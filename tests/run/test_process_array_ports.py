from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from mas.schema.fbp import fbp_capnp

from tests.component_harness import InMemoryReader, InMemoryWriter, done_message, ip_message, text_outputs
from zalfmas_fbp.run import process

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


def test_write_array_out_rejects_next_available_strategy() -> None:
    component = process.Process(metadata=_array_port_meta())
    component.array_out_ports["out"] = [cast("Any", InMemoryWriter())]

    try:
        asyncio.run(component.write_array_out("out", "next_available", _text_ip("alpha")))
    except ValueError:
        pass
    else:
        raise AssertionError("next_available is not an array output strategy")


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


async def _wait_for_state(component: process.Process, expected: ProcessStateEnum) -> None:
    async def wait() -> None:
        while component.process_state != expected:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=1)


def _standard_port_meta() -> dict[str, Any]:
    return {
        "component": {
            "info": {
                "name": "standard-port-test",
                "description": "standard port test process",
            },
            "inPorts": [{"name": "in", "contentType": "Text"}],
            "outPorts": [{"name": "out", "contentType": "Text"}],
        },
    }


def _array_port_meta() -> dict[str, Any]:
    return {
        "component": {
            "info": {
                "name": "array-port-test",
                "description": "array port test process",
            },
            "inPorts": [{"name": "items", "type": "array", "contentType": "Text"}],
            "outPorts": [{"name": "out", "type": "array", "contentType": "Text"}],
        },
    }
