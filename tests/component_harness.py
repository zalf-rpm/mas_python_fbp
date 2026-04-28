from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.process as process


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


def ip_message(content: Any) -> PortMessage:
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content)))


def done_message() -> PortMessage:
    return PortMessage(done=True)


def text_outputs(writer: InMemoryWriter) -> list[str]:
    return [value.content.as_text() for value in writer.values]
