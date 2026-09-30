from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

import capnp
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


class InFlightReader:
    """A port whose read takes the message first and only then takes its time to deliver it.

    That is how a channel read behaves: it takes the IP out of the channel, which forgets about
    it, and the IP then exists only inside that read call. Dropping such a call - canceling it or
    discarding its result - destroys the IP, while the writer upstream was already told that its
    write succeeded. Use this reader to test that a component or the runtime never gives up on a
    read it has started.
    """

    def __init__(self, messages: Sequence[PortMessage], turns: int = 5):
        self._messages = list(messages)
        self._turns = turns

    async def read(self) -> PortMessage:
        if not self._messages:
            msg = "Test component read from an exhausted input port. Add an explicit done_message()."
            raise AssertionError(msg)
        message = self._messages.pop(0)  # the IP has left the channel now
        for _ in range(self._turns):
            await asyncio.sleep(0)
        return message


class CapMessage:
    """An IP carrying a live Cap'n Proto capability, built lazily.

    pycapnp can only attach a server to a message from inside a running kj event loop, so a test
    that built one while collecting its inputs would fail with "no running event loop". The
    capability is therefore built on first access to `.value`, which a reader only does from
    inside the loop `run_process_component` sets up.

    `make_server` is called once; the instance it returns is kept on `.server` so a test can
    assert afterwards on what the fake was asked for.
    """

    def __init__(
        self,
        make_server: Callable[[], Any],
        *,
        to_attr: str | None = None,
        content_type: str | None = None,
        **attrs: Any,
    ):
        self.make_server = make_server
        self.to_attr = to_attr
        self.content_type = content_type
        self.attrs = attrs
        self.server: Any = None
        self.done = False
        self._value: PortValue | None = None

    def which(self) -> str:
        return "value"

    @property
    def value(self) -> PortValue:
        if self._value is None:
            self.server = self.make_server()
            ip = fbp_capnp.IP.new_message()
            named = dict(self.attrs)
            if self.to_attr:
                named[self.to_attr] = None  # placeholder, filled with the capability below
            else:
                ip.content = self.server
            if self.content_type:
                ip.sysAttributes.contentType = self.content_type
            if named:
                entries = ip.init("attributes", len(named))
                for i, (key, plain) in enumerate(named.items()):
                    entries[i].key = key
                    entries[i].value = self.server if key == self.to_attr else plain
            self._value = PortValue(ip)
        return self._value


def cap_message(
    make_server: Callable[[], Any],
    *,
    to_attr: str | None = None,
    content_type: str | None = None,
    **attrs: Any,
) -> CapMessage:
    """An input IP whose content - or attribute `to_attr` - is a capability served by a fake."""

    return CapMessage(make_server, to_attr=to_attr, content_type=content_type, **attrs)


@dataclass
class NoMsgResult:
    def which(self) -> str:
        return "noMsg"


NO_MSG = NoMsgResult()


class ReadIfMsgReader:
    """A port offering both blocking read() and non-blocking readIfMsg(), each drawing from its
    own explicit queue so a test can script exactly what each call sees. Use NO_MSG in the
    if_msg_messages queue for a call that should report nothing available yet, same as a real
    channel's readIfMsg does when its buffer is currently empty.
    """

    def __init__(
        self,
        read_messages: Sequence[PortMessage] = (),
        if_msg_messages: Sequence[PortMessage | NoMsgResult] = (),
    ):
        self._read_messages = list(read_messages)
        self._if_msg_messages = list(if_msg_messages)

    async def read(self) -> PortMessage:
        if not self._read_messages:
            msg = "Test component read from an exhausted input port. Add an explicit done_message()."
            raise AssertionError(msg)
        return self._read_messages.pop(0)

    async def readIfMsg(self) -> PortMessage | NoMsgResult:  # noqa: N802 - the capnp method name
        if not self._if_msg_messages:
            msg = "Test component called readIfMsg with no scripted response left."
            raise AssertionError(msg)
        return self._if_msg_messages.pop(0)


class FakeLease:
    def __init__(self, on_ack: Callable[[], None]):
        self._on_ack = on_ack

    async def ack(self) -> None:
        self._on_ack()


class LeasedResponse:
    def __init__(self, msg: PortMessage, lease: FakeLease):
        self.msg = msg
        self.lease = lease


class LeasedReader:
    """A port which offers readLeased, recording what was acknowledged.

    Set unimplemented=True to imitate a channel too old to know the method, which is how the
    runtime is supposed to discover that it has to fall back to a plain read.
    """

    def __init__(self, messages: Sequence[PortMessage], *, unimplemented: bool = False):
        self._messages = list(messages)
        self._unimplemented = unimplemented
        self.acknowledged: list[PortMessage] = []
        self.leased_reads = 0
        self.plain_reads = 0

    async def readLeased(self) -> LeasedResponse:  # noqa: N802 - the capnp method name
        self.leased_reads += 1
        if self._unimplemented:
            # The wording matters: InputRuntime._is_unimplemented falls back to looking for
            # "unimplemented" in the description, so this has to read like the real thing.
            raise capnp.KjException("unimplemented method not implemented")  # noqa: TRY003
        message = self._take()
        return LeasedResponse(message, FakeLease(lambda: self.acknowledged.append(message)))

    async def read(self) -> PortMessage:
        self.plain_reads += 1
        return self._take()

    def _take(self) -> PortMessage:
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
    after_result: Any = None
    """Whatever the `after` hook returned, if `run_process_component` was given one."""

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
    array_inputs: Mapping[str, Sequence[Sequence[PortMessage]]] | None = None,
    after: Callable[[ComponentRunResult], Coroutine[Any, Any, Any]] | None = None,
) -> ComponentRunResult:
    """Run a Process component to completion against in-memory ports.

    `after` is an async hook called with the result once the component has finished but while the
    kj event loop is still up, and its return value ends up on `result.after_result`. A capability
    an output IP carries is only callable inside that loop - once it closes the client is dead -
    so a test that wants to check what it handed downstream has to do it from here.
    """

    readers, writers = _make_ports(inputs, outputs)
    array_writers = _make_array_ports(array_outputs)
    array_readers = _make_array_readers(array_inputs)
    for name, port_readers in array_readers.items():
        component.array_in_ports[name] = cast("Any", list(port_readers))
    for name, reader in readers.items():
        component.in_ports[name] = cast("Any", reader)
    for name, writer in writers.items():
        component.out_ports[name] = cast("Any", writer)
    for name, port_writers in array_writers.items():
        component.array_out_ports[name] = cast("Any", list(port_writers))

    result = ComponentRunResult(inputs=readers, outputs=writers, array_outputs=array_writers)

    async def run_and_check() -> Any:
        await _start_process_component(component)
        return await after(result) if after is not None else None

    # inside capnp.run: a component may build or call capabilities, which needs the kj loop
    result.after_result = asyncio.run(capnp.run(run_and_check()))
    return result


async def _start_process_component(component: process.Process) -> None:
    await component.start(cast("Any", None))
    lifecycle = component.context.lifecycle
    if lifecycle.run_task is None:
        msg = "Process component did not create a run task."
        raise AssertionError(msg)
    await lifecycle.run_task
    if lifecycle.run_exception is not None:
        raise lifecycle.run_exception


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
    asyncio.run(capnp.run(run_component("test-port-infos-reader", config or {})))

    return ComponentRunResult(inputs=readers, outputs=writers, port_connector=port_connector)


def ip_message(content: Any) -> PortMessage:
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content)))


def ip_message_with_attrs(content: Any, **attrs: Any) -> PortMessage:
    ip = fbp_capnp.IP.new_message(content=content)
    if attrs:
        entries = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            entries[i].key = key
            entries[i].value = value
    return PortMessage(PortValue(ip))


def done_message() -> PortMessage:
    return PortMessage(done=True)


def text_outputs(writer: InMemoryWriter) -> list[str]:
    return [value.content.as_text() for value in writer.values]


def open_bracket_message() -> PortMessage:
    return PortMessage(PortValue(fbp_capnp.IP.new_message(type="openBracket")))


def close_bracket_message(**attrs: Any) -> PortMessage:
    ip = fbp_capnp.IP.new_message(type="closeBracket")
    if attrs:
        entries = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            entries[i].key = key
            entries[i].value = value
    return PortMessage(PortValue(ip))


def _make_ports(
    inputs: Mapping[str, Sequence[PortMessage]],
    outputs: Sequence[str],
) -> tuple[dict[str, InMemoryReader], dict[str, InMemoryWriter]]:
    # an entry may already be a reader itself, to let a test control how reads resolve
    readers = {
        name: messages if hasattr(messages, "read") else InMemoryReader(messages) for name, messages in inputs.items()
    }
    writers = {name: InMemoryWriter() for name in outputs}
    return readers, writers


def _make_array_readers(
    array_inputs: Mapping[str, Sequence[Sequence[PortMessage]]] | None,
) -> dict[str, list[Any]]:
    """One reader per slot of an array in-port, given a message list per slot."""
    if array_inputs is None:
        return {}
    return {
        name: [messages if hasattr(messages, "read") else InMemoryReader(messages) for messages in slots]
        for name, slots in array_inputs.items()
    }


def _make_array_ports(array_outputs: Mapping[str, int] | None) -> dict[str, list[InMemoryWriter]]:
    if array_outputs is None:
        return {}
    return {name: [InMemoryWriter() for _ in range(count)] for name, count in array_outputs.items()}
