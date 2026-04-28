from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Any, cast

from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.ports as ports
import zalfmas_fbp.run.process as process

type StandardComponentRunner = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


@dataclass
class PortMessage:
    value: Any = None
    done: bool = False

    def which(self) -> str:
        return "done" if self.done else "value"


@dataclass
class PortValue:
    value: Any

    def as_struct(self, _schema: Any) -> Any:
        return self.value


class InMemoryReader:
    def __init__(self, messages: Sequence[PortMessage]):
        self._messages = list(messages)

    async def read(self) -> PortMessage:
        return self._messages.pop(0)


class InMemoryWriter:
    def __init__(self):
        self.values: list[Any] = []
        self.closed = False

    async def write(self, value: Any) -> None:
        self.values.append(value)

    async def close(self) -> None:
        self.closed = True


def run_process_component(
    component: process.Process,
    *,
    in_messages: Sequence[PortMessage],
    in_port_name: str = "in",
    out_port_name: str = "out",
) -> InMemoryWriter:
    writer = InMemoryWriter()
    component.in_ports[in_port_name] = cast(Any, InMemoryReader(in_messages))
    component.out_ports[out_port_name] = cast(Any, writer)

    asyncio.run(component.run())

    return writer


def run_standard_component(
    run_component: StandardComponentRunner,
    port_connector: ports.PortConnector,
    monkeypatch: Any,
    *,
    config: dict[str, Any] | None = None,
) -> None:
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


def ip_message(content: Any) -> PortMessage:
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content)))


def done_message() -> PortMessage:
    return PortMessage(done=True)


def text_outputs(writer: InMemoryWriter) -> list[str]:
    return [value.content.as_text() for value in writer.values]
