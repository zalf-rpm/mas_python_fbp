from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, cast

import capnp

from .chunked_io import DEFAULT_BRACKETED_CHUNK_SIZE
from .chunked_io import bracket_ip as _bracket_ip
from .chunked_io import chunked_blob_ip as _chunked_blob_ip
from .chunked_io import ip_blob_payload as _ip_blob_payload
from .errors import OutputPortWriteError
from .io_runtime import ProcessRuntimeContext, wait_for_tasks_or_stop
from .types import ArrayOutStrategy

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import WriterClient

logger = logging.getLogger(__name__)

type ArrayWriterPorts = list["WriterClient | None"]
type ArrayOutWriteTasks = list["asyncio.Task[bool] | None"]


class OutputRuntime:
    def __init__(self, ctx: ProcessRuntimeContext) -> None:
        self._ctx = ctx
        self.out_ports: dict[str, WriterClient | None] = {}
        self.array_out_ports: dict[str, ArrayWriterPorts] = {}
        self.array_out_next_indices: dict[str, int] = {}
        self.array_out_write_tasks: dict[str, ArrayOutWriteTasks] = {}

    @property
    def stop_event(self) -> asyncio.Event:
        return self._ctx.stop_event

    def _output_port_rpc_error(self, port_label: str, error: capnp.KjException) -> OutputPortWriteError:
        description = str(getattr(error, "description", error))
        logger.error("%s RPC exception writing output port '%s': %s", self._ctx.name, port_label, description)
        return OutputPortWriteError(self._ctx.name, port_label, description)

    @staticmethod
    def _active_writer_ports(ports: ArrayWriterPorts) -> list[tuple[int, WriterClient]]:
        active_ports: list[tuple[int, WriterClient]] = []
        for index, port in enumerate(ports):
            if port is not None:
                active_ports.append((index, port))
        return active_ports

    def ensure_array_out_write_task_slots(
        self,
        name: str,
        ports: ArrayWriterPorts,
    ) -> ArrayOutWriteTasks:
        tasks = self.array_out_write_tasks.setdefault(name, [])
        if len(tasks) < len(ports):
            tasks.extend([None] * (len(ports) - len(tasks)))
        return tasks

    async def write_out(self, name: str, message: IPBuilder) -> bool:
        if self.stop_event.is_set():
            return False

        port = self.out_ports.get(name)
        if port is None:
            return False

        try:
            await self._ctx.transition_to_activity("waitingOutput", name)
            await port.write(value=message)
            await self._ctx.transition_to_activity("processing")
            return True
        except capnp.KjException as error:
            self.out_ports[name] = None
            if self.stop_event.is_set():
                return False
            raise self._output_port_rpc_error(name, error) from error

    async def write_out_chunked(
        self,
        name: str,
        message: IPBuilder,
        *,
        chunk_size: int = DEFAULT_BRACKETED_CHUNK_SIZE,
    ) -> bool:
        if message.type != "standard":
            msg = f"{self._ctx.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self._ctx.name, name, msg)

        try:
            data, content_type = _ip_blob_payload(message)
        except (capnp.KjException, TypeError) as error:
            msg = f"{self._ctx.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
            raise OutputPortWriteError(self._ctx.name, name, msg) from error

        chunk_count = (len(data) + chunk_size - 1) // chunk_size

        async def data_chunks():
            for offset in range(0, len(data), chunk_size):
                yield data[offset : offset + chunk_size]

        return await self.write_out_chunked_stream(
            name,
            message,
            chunks=data_chunks(),
            content_type=content_type,
            chunk_count=chunk_count,
        )

    async def write_out_chunked_stream(
        self,
        name: str,
        source: IPBuilder,
        *,
        chunks: AsyncIterable[bytes],
        content_type: str | None = None,
        chunk_count: int = 0,
    ) -> bool:
        if source.type != "standard":
            msg = f"{self._ctx.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self._ctx.name, name, msg)

        resolved_content_type = content_type
        if resolved_content_type is None:
            try:
                _data, resolved_content_type = _ip_blob_payload(source)
            except (capnp.KjException, TypeError) as error:
                msg = f"{self._ctx.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
                raise OutputPortWriteError(self._ctx.name, name, msg) from error

        port = self.out_ports.get(name)
        if port is None:
            return False

        open_ip = _bracket_ip("openBracket", source, content_type=resolved_content_type, chunk_count=chunk_count)
        close_ip = _bracket_ip("closeBracket", source, content_type=resolved_content_type, chunk_count=chunk_count)
        open_sent = False
        close_sent = False
        aborted = False
        rpc_error: capnp.KjException | None = None
        chunk_iterator = chunks.__aiter__()
        aclose = cast("Callable[[], Awaitable[object]] | None", getattr(chunk_iterator, "aclose", None))

        try:
            await self._ctx.transition_to_activity("waitingOutput", name)
            await port.write(value=open_ip)
            open_sent = True
            async for chunk in chunk_iterator:
                if self.stop_event.is_set():
                    aborted = True
                    break
                chunk_ip = _chunked_blob_ip(chunk, content_type=resolved_content_type)
                await port.write(value=chunk_ip)

            if not aborted:
                await port.write(value=close_ip)
                close_sent = True
            await self._ctx.transition_to_activity("processing")
        except capnp.KjException as error:
            rpc_error = error
        finally:
            if open_sent and not close_sent:
                with suppress(capnp.KjException, RuntimeError):
                    await port.write(value=close_ip)
                    close_sent = True

            if rpc_error is not None and self.out_ports.get(name) is port:
                self.out_ports[name] = None

            if aclose is not None:
                try:
                    await aclose()
                except RuntimeError as error:
                    logger.warning("%s chunk iterator cleanup failed on output port '%s': %s", self._ctx.name, name, error)

        if rpc_error is not None:
            if self.stop_event.is_set():
                return False
            raise self._output_port_rpc_error(name, rpc_error) from rpc_error

        return not aborted

    async def write_array_out(
        self,
        name: str,
        strategy: ArrayOutStrategy | str,
        message: IPBuilder,
    ) -> bool:
        if self.stop_event.is_set():
            return False

        ports = self.array_out_ports.get(name)
        if not ports:
            return False

        strategy = ArrayOutStrategy(strategy)
        if strategy == ArrayOutStrategy.BROADCAST:
            active_ports = self._active_writer_ports(ports)
            if not active_ports:
                return False

            write_tasks = [
                asyncio.create_task(
                    self.write_array_out_port(name, index, port, message),
                    name=f"{self._ctx.name or self._ctx.id}-{name}[{index}]-broadcast-write",
                )
                for index, port in active_ports
            ]
            try:
                results = await asyncio.gather(*write_tasks)
            except Exception:
                for task in write_tasks:
                    if not task.done():
                        task.cancel()
                _ = await asyncio.gather(*write_tasks, return_exceptions=True)
                raise
            return any(results)

        if strategy == ArrayOutStrategy.NEXT_AVAILABLE:
            return await self.write_array_out_next_available(name, ports, message)

        start_index = self.array_out_next_indices.get(name, 0)
        for offset in range(len(ports)):
            port_index = (start_index + offset) % len(ports)
            port = ports[port_index]
            if port is None:
                continue

            self.array_out_next_indices[name] = (port_index + 1) % len(ports)
            if await self.write_array_out_port(name, port_index, port, message):
                return True

        return False

    async def consume_array_out_write_task(self, name: str, port_index: int) -> bool:
        tasks = self.array_out_write_tasks.get(name)
        if tasks is None or port_index >= len(tasks):
            return False

        task = tasks[port_index]
        if task is None:
            return False

        tasks[port_index] = None
        try:
            return await task
        except asyncio.CancelledError:
            return False

    async def wait_for_next_available_array_out_port(
        self,
        name: str,
        ports: ArrayWriterPorts,
    ) -> tuple[int, WriterClient] | None:
        tasks = self.ensure_array_out_write_task_slots(name, ports)
        while not self.stop_event.is_set():
            for port_index, task in enumerate(tasks[: len(ports)]):
                if task is not None and task.done():
                    _ = await self.consume_array_out_write_task(name, port_index)

            active_ports = self._active_writer_ports(ports)
            if not active_ports:
                return None

            start_index = self.array_out_next_indices.get(name, 0)
            for offset in range(len(ports)):
                port_index = (start_index + offset) % len(ports)
                port = ports[port_index]
                if port is None:
                    continue
                if tasks[port_index] is None:
                    self.array_out_next_indices[name] = (port_index + 1) % len(ports)
                    return port_index, port

            active_tasks: dict[asyncio.Task[bool], int] = {}
            for index, _port in active_ports:
                task = tasks[index]
                if task is not None:
                    active_tasks[task] = index
            if not active_tasks:
                return None

            await self._ctx.transition_to_activity("waitingOutput", name)
            done_tasks, stopped = await wait_for_tasks_or_stop(active_tasks, self.stop_event)
            if stopped:
                return None
            await self._ctx.transition_to_activity("processing")
            for task in done_tasks:
                write_task = cast("asyncio.Task[bool]", task)
                _ = await self.consume_array_out_write_task(name, active_tasks[write_task])

        return None

    async def write_array_out_next_available(
        self,
        name: str,
        ports: ArrayWriterPorts,
        message: IPBuilder,
    ) -> bool:
        next_port = await self.wait_for_next_available_array_out_port(name, ports)
        if next_port is None:
            return False

        port_index, port = next_port
        tasks = self.ensure_array_out_write_task_slots(name, ports)
        tasks[port_index] = asyncio.create_task(
            self.write_array_out_port(name, port_index, port, message, track_activity=False),
            name=f"{self._ctx.name or self._ctx.id}-{name}[{port_index}]-write",
        )
        return True

    async def write_array_out_port(
        self,
        name: str,
        port_index: int,
        port: WriterClient,
        message: IPBuilder,
        track_activity: bool = True,
    ) -> bool:
        try:
            if track_activity:
                await self._ctx.transition_to_activity("waitingOutput", f"{name}[{port_index}]")
            await port.write(value=message)
            if track_activity:
                await self._ctx.transition_to_activity("processing")
            return True
        except capnp.KjException as error:
            self.array_out_ports[name][port_index] = None
            if self.stop_event.is_set():
                return False
            raise self._output_port_rpc_error(f"{name}[{port_index}]", error) from error

    async def finalize_array_out_write_tasks(self, *, cancel_pending: bool) -> None:
        task_refs: list[tuple[str, int, asyncio.Task[bool]]] = []
        for name, tasks in self.array_out_write_tasks.items():
            for port_index, task in enumerate(tasks):
                if task is None:
                    continue
                if task.done():
                    _ = await self.consume_array_out_write_task(name, port_index)
                    continue
                if cancel_pending:
                    _ = task.cancel()
                task_refs.append((name, port_index, task))

        if task_refs:
            _ = await asyncio.gather(*(task for _name, _port_index, task in task_refs), return_exceptions=True)
            for name, port_index, _task in task_refs:
                _ = await self.consume_array_out_write_task(name, port_index)

    async def close_out_ports(self, *, cancel_pending_writes: bool | None = None) -> None:
        if cancel_pending_writes is None:
            cancel_pending_writes = self.stop_event.is_set()
        await self.finalize_array_out_write_tasks(cancel_pending=cancel_pending_writes)

        for name, port in self.out_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.out_ports[name] = None
                    logger.info("closed out port '%s'", name)
                except (capnp.KjException, RuntimeError) as error:
                    logger.error("%s: Exception closing out port '%s': %s", Path(__file__).name, name, error)
        for name, ports in self.array_out_ports.items():
            for index, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[index] = None
                        logger.info("closed array out port '%s[%s]'", name, index)
                    except (capnp.KjException, RuntimeError) as error:
                        logger.error("Exception closing array out port '%s[%s]': %s", name, index, error)
