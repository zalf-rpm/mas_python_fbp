from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.run import ports, process

type StandardComponentRunner = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


@dataclass
class PortValue:
    value: Any

    def as_struct(self, _schema: Any) -> Any:
        return self.value


@dataclass
class PortMessage:
    value: PortValue | None = None
    done: bool = False

    def which(self) -> str:
        return "done" if self.done else "value"


class InMemoryReader:
    def __init__(self, messages: Sequence[PortMessage]):
        self._messages = list(messages)

    async def read(self) -> PortMessage:
        if not self._messages:
            msg = "Test component read from an exhausted input port. Add an explicit done_message()."
            raise AssertionError(msg)
        return self._messages.pop(0)


class InMemoryWriteRequest:
    def __init__(self, writer: InMemoryWriter):
        self._writer = writer
        self.value = PortValue(fbp_capnp.IP.new_message())

    async def send(self) -> None:
        self._writer.values.append(self.value.value)


class InMemoryWriter:
    def __init__(self):
        self.values: list[Any] = []
        self.closed = False

    async def write(self, value: Any) -> None:
        self.values.append(value)

    def write_request(self) -> InMemoryWriteRequest:
        return InMemoryWriteRequest(self)

    async def close(self) -> None:
        self.closed = True


@dataclass
class ComponentRunResult:
    inputs: dict[str, InMemoryReader]
    outputs: dict[str, InMemoryWriter]
    array_outputs: dict[str, list[InMemoryWriter]] | None = None
    port_connector: ports.PortConnector | None = None

    def output(self, name: str = "out") -> InMemoryWriter:
        return self.outputs[name]

    def array_output(self, name: str = "out") -> list[InMemoryWriter]:
        if self.array_outputs is None:
            raise KeyError(name)
        return self.array_outputs[name]


def run_process_component(
    component: process.Process,
    *,
    inputs: Mapping[str, Sequence[PortMessage]],
    outputs: Sequence[str] = ("out",),
    array_outputs: Mapping[str, int] | None = None,
) -> ComponentRunResult:
    readers, writers = _make_ports(inputs, outputs)
    array_writers = _make_array_ports(array_outputs)
    for name, reader in readers.items():
        component.in_ports[name] = cast("Any", reader)
    for name, writer in writers.items():
        component.out_ports[name] = cast("Any", writer)
    for name, port_writers in array_writers.items():
        component.array_out_ports[name] = cast("Any", list(port_writers))

    asyncio.run(_start_process_component(component))

    return ComponentRunResult(inputs=readers, outputs=writers, array_outputs=array_writers)


async def _start_process_component(component: process.Process) -> None:
    await component.start(cast("Any", None))
    if component._run_task is None:
        raise AssertionError("Process component did not create a run task.")
    await component._run_task
    if component._run_exception is not None:
        raise component._run_exception


def run_standard_component(
    run_component: StandardComponentRunner,
    monkeypatch: Any,
    *,
    inputs: Mapping[str, Sequence[PortMessage]],
    outputs: Sequence[str] = (),
    config: dict[str, Any] | None = None,
) -> ComponentRunResult:
    readers, writers = _make_ports(inputs, outputs)
    port_connector = ports.PortConnector(ins=list(readers), outs=list(writers))
    port_connector.in_ports.update(cast("dict[str, Any]", readers))
    port_connector.out_ports.update(cast("dict[str, Any]", writers))

    async def create_from_port_infos_reader(
        _port_infos_reader_sr: str,
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: Any = None,
        *,
        array_outs: Sequence[str] | None = None,
    ) -> ports.PortConnector:
        return port_connector

    monkeypatch.setattr(ports.PortConnector, "create_from_port_infos_reader", create_from_port_infos_reader)
    asyncio.run(run_component("test-port-infos-reader", config or {}))

    return ComponentRunResult(inputs=readers, outputs=writers, port_connector=port_connector)


def ip_message(content: Any) -> PortMessage:
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content)))


def done_message() -> PortMessage:
    return PortMessage(done=True)


def text_outputs(writer: InMemoryWriter) -> list[str]:
    return [value.content.as_text() for value in writer.values]


def _make_ports(
    inputs: Mapping[str, Sequence[PortMessage]],
    outputs: Sequence[str],
) -> tuple[dict[str, InMemoryReader], dict[str, InMemoryWriter]]:
    readers = {name: InMemoryReader(messages) for name, messages in inputs.items()}
    writers = {name: InMemoryWriter() for name in outputs}
    return readers, writers


def _make_array_ports(array_outputs: Mapping[str, int] | None) -> dict[str, list[InMemoryWriter]]:
    if array_outputs is None:
        return {}
    return {name: [InMemoryWriter() for _ in range(count)] for name, count in array_outputs.items()}
