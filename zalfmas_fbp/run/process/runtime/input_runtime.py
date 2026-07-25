from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast, overload

import capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.run.process.context import ProcessPortState
from zalfmas_fbp.run.process.errors import InputPortReadError
from zalfmas_fbp.run.process.identity import ProcessIdentityContext
from zalfmas_fbp.run.process.io.chunked_io import ChunkedInputStream
from zalfmas_fbp.run.process.io.chunked_io import ip_blob_payload as _ip_blob_payload
from zalfmas_fbp.run.process.io.chunked_io import ip_content_type as _ip_content_type
from zalfmas_fbp.run.process.task_utils import wait_for_tasks_or_stop
from zalfmas_fbp.run.process.types import ArrayInStrategy, ArrayReaderPorts

from .state_runtime import ProcessActivityContext

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader
    from mas.schema.fbp.fbp_capnp.types.results.client import ReadResult

logger = logging.getLogger(__name__)


def _kj_exception_description(error: capnp.KjException) -> str:
    return str(getattr(error, "description", error))


async def _cancel_tasks[T](tasks: Iterable[asyncio.Future[T]]) -> None:
    pending = list(tasks)
    for task in pending:
        _ = task.cancel()
    _ = await asyncio.gather(*pending, return_exceptions=True)


class InputRuntime:
    def __init__(
        self,
        *,
        identity: ProcessIdentityContext,
        ports: ProcessPortState,
        stop_event: asyncio.Event,
        activity: ProcessActivityContext,
    ) -> None:
        self._identity: ProcessIdentityContext = identity
        self._ports: ProcessPortState = ports
        self._stop_event: asyncio.Event = stop_event
        self._activity: ProcessActivityContext = activity

    @property
    def in_ports(self) -> dict[str, ReaderClient | None]:
        return self._ports.in_ports

    @property
    def array_in_ports(self) -> dict[str, ArrayReaderPorts]:
        return self._ports.array_in_ports

    @property
    def array_in_buffers(self) -> dict[str, dict[int, IPReader]]:
        return cast("dict[str, dict[int, IPReader]]", self._ports.array_in_buffers)

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    def _clear_in_port(self, name: str) -> None:
        self.in_ports[name] = None

    def _clear_array_in_port(self, name: str, port_index: int) -> None:
        ports = self.array_in_ports.get(name)
        if ports is not None and port_index < len(ports):
            ports[port_index] = None

    def _input_port_rpc_error(self, port_label: str, error: capnp.KjException) -> InputPortReadError:
        description = _kj_exception_description(error)
        logger.error("%s RPC exception reading input port '%s': %s", self._identity.name, port_label, description)
        return InputPortReadError(self._identity.name, port_label, description)

    @staticmethod
    def _active_reader_ports(ports: ArrayReaderPorts) -> list[tuple[int, ReaderClient]]:
        active_ports: list[tuple[int, ReaderClient]] = []
        for index, port in enumerate(ports):
            if port is not None:
                active_ports.append((index, port))
        return active_ports

    async def read_in(self, name: str) -> IPReader | None:
        in_ip = await self.read_in_raw(name)
        if in_ip is None:
            return None

        if in_ip.type == "openBracket" and in_ip.sysAttributes.bracketType.which() in ("chunkedContent", "chunkedIp"):
            msg = f"{self._identity.name} received a chunked payload openBracket on input port '{name}'; use read_in_chunked* instead."
            raise InputPortReadError(self._identity.name, name, msg)

        if in_ip.type == "closeBracket" and in_ip.sysAttributes.bracketType.which() in ("chunkedContent", "chunkedIp"):
            msg = f"{self._identity.name} received an unexpected chunked payload closeBracket on input port '{name}'; use read_in_chunked* instead."
            raise InputPortReadError(self._identity.name, name, msg)

        return in_ip

    async def read_in_chunked(self, name: str) -> IPReader | None:
        in_ip = await self.read_in_raw(name)
        if in_ip is None:
            return None
        return await self.coalesce_chunked_input(name, in_ip, lambda: self.read_in_raw(name))

    async def read_in_chunked_stream(self, name: str) -> ChunkedInputStream | None:
        in_ip = await self.read_in_raw(name)
        if in_ip is None:
            return None
        return await self.chunked_stream_for_ip(name, in_ip, lambda: self.read_in_raw(name))

    async def read_connected_port(
        self,
        *,
        port: ReaderClient,
        port_label: str,
        on_disconnect: Callable[[], None],
    ) -> IPReader | None:
        if self.stop_event.is_set():
            return None

        await self._activity.transition_to_activity("waitingInput", port_label)
        read_task = asyncio.ensure_future(port.read())
        try:
            done_tasks, stopped = await wait_for_tasks_or_stop({read_task}, self.stop_event)
            if stopped:
                if read_task not in done_tasks:
                    await _cancel_tasks((read_task,))
                    return None
                _ = await read_task
                return None

            msg = await read_task
            await self._activity.transition_to_activity("processing")
            if msg.which() == "done":
                on_disconnect()
                return None
            return msg.value.as_struct(fbp_capnp.IP)
        except asyncio.CancelledError:
            await _cancel_tasks((read_task,))
            raise
        except capnp.KjException as error:
            on_disconnect()
            if self.stop_event.is_set():
                return None
            raise self._input_port_rpc_error(port_label, error) from error

    async def read_in_raw(self, name: str) -> IPReader | None:
        port = self.in_ports.get(name)
        if port is None:
            return None
        return await self.read_connected_port(
            port=port,
            port_label=name,
            on_disconnect=lambda: self._clear_in_port(name),
        )

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | None: ...

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
    ) -> IPReader | None: ...

    async def read_array_in(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | IPReader | None:
        return await self.read_array_in_any(name, strategy)

    async def read_array_in_any(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | IPReader | None:
        strategy = ArrayInStrategy(strategy)
        if self.stop_event.is_set():
            return None

        ports = self.array_in_ports.get(name)
        if not ports:
            return None

        active_ports = self._active_reader_ports(ports)
        if not active_ports:
            return None

        if strategy == ArrayInStrategy.NEXT_AVAILABLE:
            next_result = await self.read_array_in_next_available(name, active_ports, ports)
            if next_result is None:
                return None

            port_index, in_ip = next_result
            await self._activity.transition_to_activity("processing")
            if in_ip.type == "openBracket" and in_ip.sysAttributes.bracketType.which() in (
                "chunkedContent",
                "chunkedIp",
            ):
                msg = (
                    f"{self._identity.name} received a chunked payload on array input port '{name}[{port_index}]'; "
                    "use read_array_in_chunked instead."
                )
                raise InputPortReadError(self._identity.name, f"{name}[{port_index}]", msg)
            if in_ip.type == "closeBracket" and in_ip.sysAttributes.bracketType.which() in (
                "chunkedContent",
                "chunkedIp",
            ):
                msg = f"{self._identity.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self._identity.name, f"{name}[{port_index}]", msg)
            return in_ip

        ordered_results = await self.read_array_zip_raw(name, active_ports, ports)
        if ordered_results is None:
            return None

        bracketed_results = [ip for ip in ordered_results if ip.type in ("openBracket", "closeBracket")]
        if bracketed_results:
            msg = f"{self._identity.name} received a chunked payload on array input port '{name}'; use read_array_in_chunked."
            raise InputPortReadError(self._identity.name, name, msg)
        return ordered_results

    async def read_array_in_next_available(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
    ) -> tuple[int, IPReader] | None:
        buffers = self.array_in_buffers.setdefault(name, {})
        if buffers:
            port_index = next(iter(buffers))
            return port_index, buffers.pop(port_index)

        while active_ports:
            read_tasks: dict[asyncio.Future[ReadResult], int] = {
                asyncio.ensure_future(port.read()): index for index, port in active_ports
            }
            await self._activity.transition_to_activity("waitingInput", name)

            try:
                done_tasks, stopped = await wait_for_tasks_or_stop(read_tasks, self.stop_event)
                if stopped:
                    await _cancel_tasks(read_tasks)
                    return None

                for task in done_tasks:
                    port_index = read_tasks.pop(task)
                    try:
                        msg = await task
                    except capnp.KjException as error:
                        ports[port_index] = None
                        if self.stop_event.is_set():
                            continue
                        msg_0 = f"{name}[{port_index}]"
                        raise self._input_port_rpc_error(msg_0, error) from error

                    if msg.which() == "done":
                        ports[port_index] = None
                    else:
                        buffers[port_index] = msg.value.as_struct(fbp_capnp.IP)

                await _cancel_tasks(read_tasks)
                if buffers:
                    port_index = next(iter(buffers))
                    return port_index, buffers.pop(port_index)

                active_ports = self._active_reader_ports(ports)
            except asyncio.CancelledError:
                await _cancel_tasks(read_tasks)
                raise
            except Exception:
                await _cancel_tasks(read_tasks)
                raise
        return None

    @overload
    async def read_array_in_chunked(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | None: ...

    @overload
    async def read_array_in_chunked(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
    ) -> IPReader | None: ...

    async def read_array_in_chunked(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | IPReader | None:
        return await self.read_array_in_chunked_any(name, strategy)

    async def read_array_in_chunked_any(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | IPReader | None:
        strategy = ArrayInStrategy(strategy)
        if self.stop_event.is_set():
            return None

        ports = self.array_in_ports.get(name)
        if not ports:
            return None

        active_ports = self._active_reader_ports(ports)
        if not active_ports:
            return None

        if strategy == ArrayInStrategy.NEXT_AVAILABLE:
            next_result = await self.read_array_in_next_available(name, active_ports, ports)
            if next_result is None:
                return None

            port_index, in_ip = next_result
            await self._activity.transition_to_activity("processing")
            port = ports[port_index]
            read_next_ip = (
                (lambda: self.read_array_port_raw(name, port_index, port))
                if in_ip.type == "openBracket" and port is not None
                else (lambda: self.read_in_raw(name))
            )
            return await self.coalesce_chunked_input(f"{name}[{port_index}]", in_ip, read_next_ip)

        ordered_results = await self.read_array_zip_raw(name, active_ports, ports)
        if ordered_results is None:
            return None
        return await self.coalesce_chunked_array_results(name, active_ports, ordered_results)

    async def read_array_zip_raw(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
    ) -> list[IPReader] | None:
        read_tasks: dict[asyncio.Future[ReadResult], int] = {
            asyncio.ensure_future(port.read()): index for index, port in active_ports
        }
        results: dict[int, IPReader] = {}
        zip_finished = False
        await self._activity.transition_to_activity("waitingInput", name)

        try:
            while read_tasks:
                done_tasks, stopped = await wait_for_tasks_or_stop(read_tasks, self.stop_event)
                if stopped:
                    await _cancel_tasks(read_tasks)
                    return None

                for task in done_tasks:
                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as error:
                        ports[port_index] = None
                        if self.stop_event.is_set():
                            zip_finished = True
                            continue
                        msg_0 = f"{name}[{port_index}]"
                        raise self._input_port_rpc_error(msg_0, error) from error

                    if msg.which() == "done":
                        ports[port_index] = None
                        zip_finished = True
                    else:
                        results[port_index] = msg.value.as_struct(fbp_capnp.IP)

                if zip_finished:
                    await _cancel_tasks(read_tasks)
                    await self._activity.transition_to_activity("processing")
                    return None

            await self._activity.transition_to_activity("processing")
            return [results[index] for index, _port in active_ports]
        except asyncio.CancelledError:
            await _cancel_tasks(read_tasks)
            raise
        except Exception:
            await _cancel_tasks(read_tasks)
            raise

    async def coalesce_chunked_array_results(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        first_messages: list[IPReader],
    ) -> list[IPReader]:
        coalesced: list[IPReader] = []
        for (port_index, port), first_ip in zip(active_ports, first_messages, strict=True):
            if first_ip.type == "openBracket":
                coalesced.append(
                    await self.coalesce_chunked_input(
                        f"{name}[{port_index}]",
                        first_ip,
                        lambda port=port, port_index=port_index: self.read_array_port_raw(name, port_index, port),
                    ),
                )
            elif first_ip.type == "closeBracket":
                msg = f"{self._identity.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self._identity.name, f"{name}[{port_index}]", msg)
            else:
                coalesced.append(first_ip)
        return coalesced

    async def read_array_port_raw(
        self,
        name: str,
        port_index: int,
        port: ReaderClient,
    ) -> IPReader | None:
        return await self.read_connected_port(
            port=port,
            port_label=f"{name}[{port_index}]",
            on_disconnect=lambda: self._clear_array_in_port(name, port_index),
        )

    async def coalesce_chunked_input(
        self,
        port_label: str,
        in_ip: IPReader,
        read_next_ip: Callable[[], Awaitable[IPReader | None]],
    ) -> IPReader:
        if in_ip.type == "openBracket":
            stream = await self.chunked_stream_for_ip(port_label, in_ip, read_next_ip)
            return await stream.collect_blob()
        if in_ip.type == "closeBracket":
            msg = f"{self._identity.name} received an unexpected closeBracket on input port '{port_label}'."
            raise InputPortReadError(self._identity.name, port_label, msg)
        return in_ip

    async def chunked_stream_for_ip(
        self,
        port_label: str,
        in_ip: IPReader,
        read_next_ip: Callable[[], Awaitable[IPReader | None]],
    ) -> ChunkedInputStream:
        if in_ip.type == "closeBracket":
            msg = f"{self._identity.name} received an unexpected closeBracket on input port '{port_label}'."
            raise InputPortReadError(self._identity.name, port_label, msg)

        if in_ip.type == "openBracket":
            return ChunkedInputStream(
                open_ip=in_ip,
                process_name=self._identity.name,
                port=port_label,
                _read_next_ip=read_next_ip,
                _is_stopping=self.stop_event.is_set,
                _on_complete=lambda: self._activity.transition_to_activity("processing", delay_processing=False),
                content_type=_ip_content_type(in_ip),
            )

        try:
            data, content_type = _ip_blob_payload(in_ip)
        except (capnp.KjException, TypeError) as error:
            msg = (
                f"{self._identity.name} can only read common.capnp:Blob chunked payloads on input port '{port_label}'."
            )
            raise InputPortReadError(self._identity.name, port_label, msg) from error

        return ChunkedInputStream(
            open_ip=in_ip,
            process_name=self._identity.name,
            port=port_label,
            _read_next_ip=lambda: asyncio.sleep(0, result=None),
            _is_stopping=self.stop_event.is_set,
            _on_complete=None,
            _single_chunk=data,
            content_type=content_type,
        )

    async def close_in_ports(self) -> None:
        for name, port in self.in_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.in_ports[name] = None
                    logger.info("closed in port '%s'", name)
                except (capnp.KjException, RuntimeError):
                    logger.exception("%s: Exception closing in port '%s'", Path(__file__).name, name)
        for name, ports in self.array_in_ports.items():
            for index, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[index] = None
                        logger.info("closed array in port '%s[%s]'", name, index)
                    except (capnp.KjException, RuntimeError):
                        logger.exception("Exception closing array in port '%s[%s]'", name, index)
