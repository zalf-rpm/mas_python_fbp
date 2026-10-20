from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, cast

import capnp

from zalfmas_fbp.run.metadata import CONFIG_PORT_NAME, LOG_PORT_NAME
from zalfmas_fbp.run.process.context import ProcessPortState
from zalfmas_fbp.run.process.errors import OutputPortWriteError
from zalfmas_fbp.run.process.identity import ProcessIdentityContext
from zalfmas_fbp.run.process.io.chunked_io import DEFAULT_BRACKETED_CHUNK_SIZE
from zalfmas_fbp.run.process.io.chunked_io import bracket_ip as _bracket_ip
from zalfmas_fbp.run.process.io.chunked_io import chunked_blob_ip as _chunked_blob_ip
from zalfmas_fbp.run.process.io.chunked_io import ip_blob_payload as _ip_blob_payload
from zalfmas_fbp.run.process.task_utils import wait_for_tasks_or_stop
from zalfmas_fbp.run.process.types import ArrayOutStrategy, ArrayOutWriteTasks, ArrayWriterPorts

from .state_runtime import ProcessActivityContext

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import WriterClient
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)


class OutputRuntime:
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
        # Set by the runtime's ConfigWatcher; called once an IP has been written, so staged config
        # lands between IPs. Reads have the same hook - a component with no data in-port, such as a
        # file reader, would otherwise never reach a boundary at all.
        self.apply_pending_config: Callable[[], bool] | None = None
        self.config_port_name: str = CONFIG_PORT_NAME
        self.log_port_name: str = LOG_PORT_NAME

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    @property
    def out_ports(self) -> dict[str, WriterClient | None]:
        return self._ports.out_ports

    @property
    def array_out_ports(self) -> dict[str, ArrayWriterPorts]:
        return self._ports.array_out_ports

    @property
    def array_out_next_indices(self) -> dict[str, int]:
        return self._ports.array_out_next_indices

    @property
    def array_out_write_tasks(self) -> dict[str, ArrayOutWriteTasks]:
        return self._ports.array_out_write_tasks

    def _output_port_rpc_error(self, port_label: str, error: capnp.KjException) -> OutputPortWriteError:
        description = str(getattr(error, "description", error))
        logger.error("%s RPC exception writing output port '%s': %s", self._identity.name, port_label, description)
        return OutputPortWriteError(self._identity.name, port_label, description)

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

    async def write_out(self, name: str, message: IPBuilder | IPReader) -> bool:
        if self.stop_event.is_set():
            return False

        port = self.out_ports.get(name)
        if port is None:
            return False

        try:
            await self._activity.transition_to_activity("waitingOutput", name)
            await port.write(value=message)
            await self._activity.transition_to_activity("processing")
        except capnp.KjException as error:
            self.out_ports[name] = None
            if self.stop_event.is_set():
                return False
            raise self._output_port_rpc_error(name, error) from error
        else:
            self._apply_pending_config_for(name)
            return True

    def _apply_pending_config_for(self, name: str) -> None:
        """An IP has left the component, so staged config may take effect (plan section 6.1).

        The log port is skipped: the tee writes there from its own task, and config application is
        the component's boundary, not the tee's.
        """
        if self.apply_pending_config is None or name in (self.config_port_name, self.log_port_name):
            return
        _ = self.apply_pending_config()

    async def write_out_if_space(self, name: str, message: IPBuilder | IPReader) -> bool:
        """Write without ever blocking: if the channel buffer is full, drop the message.

        Required for the runtime-owned ``log`` port (plan section 6.2). A blocking ``write`` there
        would let a slow or stalled log consumer stall the flow it is observing, and a consumer
        placed downstream in the same flow could deadlock it outright.
        """
        if self.stop_event.is_set():
            return False
        port = self.out_ports.get(name)
        if port is None:
            return False

        try:
            response = await port.writeIfSpace(value=message)
        except capnp.KjException as error:
            # A channel that predates writeIfSpace reports it as unimplemented. Dropping is the
            # right answer either way: this path exists precisely so it can never block.
            logger.debug("%s: writeIfSpace on port %r failed: %s", self._identity.name, name, error)
            return False
        return bool(getattr(response, "success", False))

    async def write_out_chunked(
        self,
        name: str,
        message: IPBuilder | IPReader,
        *,
        chunk_size: int = DEFAULT_BRACKETED_CHUNK_SIZE,
    ) -> bool:
        if message.type != "standard":
            msg = f"{self._identity.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self._identity.name, name, msg)

        try:
            data, content_type = _ip_blob_payload(message)
        except (capnp.KjException, TypeError) as error:
            msg = f"{self._identity.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
            raise OutputPortWriteError(self._identity.name, name, msg) from error

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
        source: IPBuilder | IPReader,
        *,
        chunks: AsyncIterable[bytes],
        content_type: str | None = None,
        chunk_count: int = 0,
    ) -> bool:
        if source.type != "standard":
            msg = f"{self._identity.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self._identity.name, name, msg)

        resolved_content_type = content_type
        if resolved_content_type is None:
            try:
                _data, resolved_content_type = _ip_blob_payload(source)
            except (capnp.KjException, TypeError) as error:
                msg = f"{self._identity.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
                raise OutputPortWriteError(self._identity.name, name, msg) from error

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
            await self._activity.transition_to_activity("waitingOutput", name)
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
            await self._activity.transition_to_activity("processing")
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
                    _ = await aclose()
                except RuntimeError as error:
                    logger.warning(
                        "%s chunk iterator cleanup failed on output port '%s': %s",
                        self._identity.name,
                        name,
                        error,
                    )

        if rpc_error is not None:
            if self.stop_event.is_set():
                return False
            raise self._output_port_rpc_error(name, rpc_error) from rpc_error

        return not aborted

    async def write_array_out(
        self,
        name: str,
        strategy: ArrayOutStrategy | str,
        message: IPBuilder | IPReader,
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
                    name=f"{self._identity.name or self._identity.id}-{name}[{index}]-broadcast-write",
                )
                for index, port in active_ports
            ]
            try:
                results = await asyncio.gather(*write_tasks)
            except Exception:
                for task in write_tasks:
                    if not task.done():
                        _ = task.cancel()
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

    async def choose_array_out_index(self, name: str, strategy: ArrayOutStrategy | str) -> int | None:
        """Pick a slot of an array out-port without writing to it yet.

        A component sending a *sequence* of IPs to one slot - a whole substream, say - has to choose
        once and then keep writing there; the write_array_out strategies choose per message.
        """
        ports = self.array_out_ports.get(name)
        if not ports:
            return None

        if ArrayOutStrategy(strategy) == ArrayOutStrategy.NEXT_AVAILABLE:
            chosen = await self.wait_for_next_available_array_out_port(name, ports)
            return None if chosen is None else chosen[0]

        start_index = self.array_out_next_indices.get(name, 0)
        for offset in range(len(ports)):
            port_index = (start_index + offset) % len(ports)
            if ports[port_index] is not None:
                self.array_out_next_indices[name] = (port_index + 1) % len(ports)
                return port_index
        return None

    async def write_array_out_at(self, name: str, index: int, message: IPBuilder | IPReader) -> bool:
        """Write to one specific slot of an array out-port.

        The ``ArrayOutStrategy`` variants all choose the slot themselves; a component routing by
        content has already decided which one it wants.
        """
        if self.stop_event.is_set():
            return False
        ports = self.array_out_ports.get(name)
        if not ports or not (0 <= index < len(ports)):
            return False
        port = ports[index]
        if port is None:
            return False
        return await self.write_array_out_port(name, index, port, message)

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

            await self._activity.transition_to_activity("waitingOutput", name)
            done_tasks, stopped = await wait_for_tasks_or_stop(active_tasks, self.stop_event)
            if stopped:
                return None
            await self._activity.transition_to_activity("processing")
            for task in done_tasks:
                write_task = cast("asyncio.Task[bool]", task)
                _ = await self.consume_array_out_write_task(name, active_tasks[write_task])

        return None

    async def write_array_out_next_available(
        self,
        name: str,
        ports: ArrayWriterPorts,
        message: IPBuilder | IPReader,
    ) -> bool:
        next_port = await self.wait_for_next_available_array_out_port(name, ports)
        if next_port is None:
            return False

        port_index, port = next_port
        tasks = self.ensure_array_out_write_task_slots(name, ports)
        tasks[port_index] = asyncio.create_task(
            self.write_array_out_port(name, port_index, port, message, track_activity=False),
            name=f"{self._identity.name or self._identity.id}-{name}[{port_index}]-write",
        )
        return True

    async def write_array_out_port(
        self,
        name: str,
        port_index: int,
        port: WriterClient,
        message: IPBuilder | IPReader,
        track_activity: bool = True,
    ) -> bool:
        try:
            if track_activity:
                await self._activity.transition_to_activity("waitingOutput", f"{name}[{port_index}]")
            await port.write(value=message)
            if track_activity:
                await self._activity.transition_to_activity("processing")
        except capnp.KjException as error:
            self.array_out_ports[name][port_index] = None
            if self.stop_event.is_set():
                return False
            msg = f"{name}[{port_index}]"
            raise self._output_port_rpc_error(msg, error) from error
        else:
            return True

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
                except (capnp.KjException, RuntimeError):
                    logger.exception("%s: Exception closing out port '%s'", Path(__file__).name, name)
        for name, ports in self.array_out_ports.items():
            for index, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[index] = None
                        logger.info("closed array out port '%s[%s]'", name, index)
                    except (capnp.KjException, RuntimeError):
                        logger.exception("Exception closing array out port '%s[%s]'", name, index)
