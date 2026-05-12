# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, Mapping
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Literal,
    cast,
    get_args,
    get_origin,
    overload,
    override,
)

import capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.fbp.fbp_capnp.types.results.tuples import (
    ConnectinportResultTuple,
    ConnectoutportResultTuple,
)

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import ActivityInfoBuilder, IPBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import (
        ActivityTransitionClient,
        ReaderClient,
        StateTransitionClient,
        WriterClient,
    )
    from mas.schema.fbp.fbp_capnp.types.enums import (
        ProcessActivityStateEnum,
        ProcessErrorInfoPhaseEnum,
        ProcessStateEnum,
    )
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader


from zalfmas_common import common

from zalfmas_fbp.run.logging_config import (
    configure_logging,
    format_exception_full,
)
from zalfmas_fbp.run.metadata import ComponentMetadata, ComponentPortMetadata

from . import input_runtime as _input_runtime
from . import output_runtime as _output_runtime
from .chunked_io import DEFAULT_BRACKETED_CHUNK_SIZE, ChunkedInputStream
from .config_codec import (
    config_from_ip as _config_from_ip,
)
from .config_codec import (
    config_value_from_python,
    python_value_from_capnp_value,
)
from .errors import (
    ProcessConfigError,
    ProcessErrorInfo,
)
from .transitions import PortDisconnect
from .types import (
    ArrayInStrategy,
    ArrayOutStrategy,
    ConfigValue,
    ProcessConfig,
    RawConfig,
)

ArrayReaderPorts = list["ReaderClient | None"]
ArrayWriterPorts = list["WriterClient | None"]
ArrayOutWriteTasks = list["asyncio.Task[bool] | None"]
DEFAULT_SOFT_STOP_TIMEOUT_SECONDS = 30.0
DEFAULT_PROCESSING_ACTIVITY_DELAY_MILLISECONDS = 25
logger = logging.getLogger(__name__)
configure_logging()


class Process[ConfigT: ProcessConfig | RawConfig](  # pyright: ignore[reportUnsafeMultipleInheritance]
    fbp_capnp.Process.Server,
    common.Identifiable,
    common.GatewayRegistrable,
):
    config_model: ClassVar[type[ProcessConfig] | None] = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is Process:
                config_type = get_args(base)[0]
                cls.config_model = (
                    config_type if isinstance(config_type, type) and issubclass(config_type, ProcessConfig) else None
                )
                return

    def __init__(
        self,
        metadata: ComponentMetadata | None = None,
        con_man: common.ConnectionManager | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        common.GatewayRegistrable.__init__(self, con_man or common.ConnectionManager())

        self.metadata: ComponentMetadata | None = metadata
        self._raw_config: RawConfig = {}
        self._config: ConfigT | RawConfig = self._raw_config
        self._run_task: asyncio.Task[None] | None = None
        self._run_exception: BaseException | None = None
        self._last_error: ProcessErrorInfo | None = None
        self._stop_requested: asyncio.Event = asyncio.Event()
        self._input_runtime = _input_runtime.InputRuntime(self)
        self._output_runtime = _output_runtime.OutputRuntime(self)
        self.soft_stop_timeout_seconds: float = DEFAULT_SOFT_STOP_TIMEOUT_SECONDS
        self.process_state: ProcessStateEnum = "idle"
        self.state_transition_callbacks: list[StateTransitionClient] = []
        self.activity_state: ProcessActivityStateEnum = "none"
        self.activity_port: str = ""
        self.activity_transition_callbacks: list[ActivityTransitionClient] = []
        self._pending_processing_activity_task: asyncio.Task[None] | None = None

        self.init_from_metadata()

    @staticmethod
    def _is_array_port(port_info: ComponentPortMetadata) -> bool:
        return port_info.type == "array"

    @staticmethod
    def _port_message(name: str, port_info: ComponentPortMetadata | None, port_type: str):
        return {
            "name": name,
            "type": port_type,
            "contentType": port_info.contentType if port_info is not None else "Text",
        }

    def _validate_config(self, raw_config: RawConfig) -> ConfigT | RawConfig:
        if self.config_model is None:
            return raw_config
        return cast("ConfigT", self.config_model.model_validate(raw_config))

    def apply_config_values(self, config_values: Mapping[str, ConfigValue | None]) -> None:
        next_raw_config = self._raw_config.copy()
        for key, value in config_values.items():
            if value is None:
                _ = next_raw_config.pop(key, None)
                continue
            next_raw_config[key] = value

        next_config = self._validate_config(next_raw_config)
        self._raw_config = next_raw_config
        self._config = next_config

    def _record_error(self, exc: BaseException) -> None:
        self._run_exception = exc
        cause = exc.__cause__ or exc.__context__
        self._last_error = ProcessErrorInfo(
            process_id=self.id,
            process_name=self.name,
            phase=cast("str", getattr(exc, "phase", "run")),
            port=cast("str | None", getattr(exc, "port", None)),
            error_type=type(exc).__name__,
            message=str(exc),
            cause_type=type(cause).__name__ if cause is not None else None,
            cause_message=str(cause) if cause is not None else None,
            traceback=format_exception_full(exc),
        )

    @staticmethod
    def _schema_error_phase(phase: str) -> ProcessErrorInfoPhaseEnum:
        match phase:
            case "config":
                return "config"
            case "read":
                return "read"
            case "run":
                return "run"
            case "write":
                return "write"
            case "close":
                return "close"
            case _:
                return "unknown"

    def _last_error_message(self):
        if self._last_error is None:
            return fbp_capnp.Process.ErrorInfo.new_message(hasError=False)

        return fbp_capnp.Process.ErrorInfo.new_message(
            hasError=True,
            processId=self._last_error.process_id or "",
            processName=self._last_error.process_name or "",
            phase=self._schema_error_phase(self._last_error.phase),
            port=self._last_error.port or "",
            errorType=self._last_error.error_type,
            message=self._last_error.message,
            causeType=self._last_error.cause_type or "",
            causeMessage=self._last_error.cause_message or "",
            traceback=self._last_error.traceback or [],
        )

    def init_from_metadata(self):
        default_config: dict[str, ConfigValue] = {}
        if self.meta is not None:
            try:
                component_meta = self.meta
                default_config = component_meta.default_config_values()
                self.name = component_meta.info.name
                if component_meta.info.description is not None:
                    self.description = component_meta.info.description
                for port_info in component_meta.inPorts:
                    name = port_info.name
                    if self._is_array_port(port_info):
                        self.array_in_ports.setdefault(name, [])
                        self._array_in_buffers.setdefault(name, {})
                    else:
                        self.in_ports.setdefault(name, None)
                for port_info in component_meta.outPorts:
                    name = port_info.name
                    if self._is_array_port(port_info):
                        self.array_out_ports.setdefault(name, [])
                        self._array_out_next_indices.setdefault(name, 0)
                        self._array_out_write_tasks.setdefault(name, [])
                    else:
                        self.out_ports.setdefault(name, None)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(
                    "Some metadata could not be used for initializing the process component. Exception: %s",
                    e,
                )
        for key, value in default_config.items():
            if value is None:
                continue
            self._raw_config[key] = value
        self._config = self._validate_config(self._raw_config)

    @property
    def meta(self) -> ComponentMetadata | None:
        return self.metadata

    @property
    def config(self) -> ConfigT:
        return cast("ConfigT", self._config)

    @property
    def raw_config(self) -> RawConfig:
        return self._raw_config

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_requested

    @property
    def in_ports(self) -> dict[str, ReaderClient | None]:
        return self._input_runtime.in_ports

    @property
    def array_in_ports(self) -> dict[str, ArrayReaderPorts]:
        return self._input_runtime.array_in_ports

    @property
    def _array_in_buffers(self) -> dict[str, dict[int, IPReader]]:
        return self._input_runtime.array_in_buffers

    @property
    def out_ports(self) -> dict[str, WriterClient | None]:
        return self._output_runtime.out_ports

    @property
    def array_out_ports(self) -> dict[str, ArrayWriterPorts]:
        return self._output_runtime.array_out_ports

    @property
    def _array_out_next_indices(self) -> dict[str, int]:
        return self._output_runtime.array_out_next_indices

    @property
    def _array_out_write_tasks(self) -> dict[str, ArrayOutWriteTasks]:
        return self._output_runtime.array_out_write_tasks

    # inPorts @0 () -> (ports :List(Component.Port));
    @override
    async def inPorts(self, _context, **kwargs):
        component_meta = self.meta
        in_port_infos = {p.name: p for p in component_meta.inPorts} if component_meta is not None else {}
        ports = [self._port_message(k, in_port_infos.get(k), "standard") for k in self.in_ports]
        ports.extend(self._port_message(k, in_port_infos.get(k), "array") for k in self.array_in_ports)
        return ports

    # connectInPort @1 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    @override
    async def connectInPort(self, name: str, sturdyRef, _context, **kwargs) -> ConnectinportResultTuple:
        reader = (
            reader_cap.cast_as(fbp_capnp.Channel.Reader)
            if (reader_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        if name in self.array_in_ports:
            index = len(self.array_in_ports[name])
            self.array_in_ports[name].append(reader)
            self._array_in_buffers.setdefault(name, {})
            return ConnectinportResultTuple(
                reader is not None,
                PortDisconnect(self.array_in_ports, name, reader, index),
            )

        self.in_ports[name] = reader
        return ConnectinportResultTuple(
            self.in_ports[name] is not None,
            PortDisconnect(self.in_ports, name, reader),
        )

    # outPorts @2 () -> (ports :List(Component.Port));
    @override
    async def outPorts(self, _context, **kwargs):
        component_meta = self.meta
        out_port_infos = {p.name: p for p in component_meta.outPorts} if component_meta is not None else {}
        ports = [self._port_message(k, out_port_infos.get(k), "standard") for k in self.out_ports]
        ports.extend(self._port_message(k, out_port_infos.get(k), "array") for k in self.array_out_ports)
        return ports

    # connectOutPort @3 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    @override
    async def connectOutPort(self, name: str, sturdyRef, _context, **kwargs) -> ConnectoutportResultTuple:
        writer = (
            writer_cap.cast_as(fbp_capnp.Channel.Writer)
            if (writer_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        if name in self.array_out_ports:
            index = len(self.array_out_ports[name])
            self.array_out_ports[name].append(writer)
            self._array_out_next_indices.setdefault(name, 0)
            self._output_runtime.ensure_array_out_write_task_slots(name, self.array_out_ports[name])
            return ConnectoutportResultTuple(
                writer is not None,
                PortDisconnect(self.array_out_ports, name, writer, index),
            )

        self.out_ports[name] = writer
        return ConnectoutportResultTuple(
            self.out_ports[name] is not None,
            PortDisconnect(self.out_ports, name, writer),
        )

    # configEntries @4 () -> (config :List(ConfigEntry));
    @override
    async def configEntries(self, _context, **kwargs):
        return list(
            map(
                lambda item: fbp_capnp.Process.ConfigEntry.new_message(
                    name=item[0],
                    val=config_value_from_python(item[1]),
                ),
                self.raw_config.items(),
            ),
        )

    @override
    async def setConfigEntry(self, name, val, _context, **kwargs):
        self.apply_config_values({name: python_value_from_capnp_value(val)})

    async def transition_to_state(self, new_state: ProcessStateEnum):
        if new_state == self.process_state:
            return

        prev_state = self.process_state
        self.process_state = new_state
        await self._notify_state_transition_callbacks(prev_state, self.process_state)

    def _activity_message(self) -> ActivityInfoBuilder:
        return fbp_capnp.Process.ActivityInfo.new_message(state=self.activity_state, port=self.activity_port)

    async def _notify_state_transition_callbacks(
        self,
        old_state: ProcessStateEnum,
        new_state: ProcessStateEnum,
    ) -> None:
        active_callbacks: list[StateTransitionClient] = []
        for callback in self.state_transition_callbacks:
            try:
                await callback.stateChanged(old_state, new_state)
            except (capnp.KjException, RuntimeError) as e:
                logger.warning("%s state transition callback failed and will be removed: %s", self.name, e)
            else:
                active_callbacks.append(callback)
        self.state_transition_callbacks = active_callbacks

    async def _notify_activity_transition_callbacks(
        self,
        old_info: ActivityInfoBuilder,
        new_info: ActivityInfoBuilder,
    ) -> None:
        active_callbacks: list[ActivityTransitionClient] = []
        for callback in self.activity_transition_callbacks:
            try:
                await callback.activityChanged(old_info, new_info)
            except (capnp.KjException, RuntimeError) as e:
                logger.warning("%s activity transition callback failed and will be removed: %s", self.name, e)
            else:
                active_callbacks.append(callback)
        self.activity_transition_callbacks = active_callbacks

    def _cancel_pending_processing_activity(self) -> None:
        task = self._pending_processing_activity_task
        self._pending_processing_activity_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _transition_to_activity_now(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
    ) -> None:
        new_port = port or ""
        if new_state == self.activity_state and new_port == self.activity_port:
            return

        old_info = self._activity_message()
        self.activity_state = new_state
        self.activity_port = new_port
        new_info = self._activity_message()
        await self._notify_activity_transition_callbacks(old_info, new_info)

    async def _commit_processing_activity_after_delay(self, delay_milliseconds: int) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(delay_milliseconds / 1000)
            if self._pending_processing_activity_task is not task:
                return
            self._pending_processing_activity_task = None
            await self._transition_to_activity_now("processing")
        except asyncio.CancelledError:
            pass

    async def transition_to_activity(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
        delay_processing: bool = True,
    ) -> None:
        if new_state == "processing":
            if self.activity_state == "processing" and self.activity_port == "":
                return
            self._cancel_pending_processing_activity()
            if not delay_processing or DEFAULT_PROCESSING_ACTIVITY_DELAY_MILLISECONDS <= 0:
                await self._transition_to_activity_now("processing")
                return
            self._pending_processing_activity_task = asyncio.create_task(
                self._commit_processing_activity_after_delay(DEFAULT_PROCESSING_ACTIVITY_DELAY_MILLISECONDS),
                name=f"{self.name or self.id}-processing-activity-delay",
            )
            return

        self._cancel_pending_processing_activity()
        await self._transition_to_activity_now(new_state, port)

    # start @5 ();
    @override
    async def start(self, _context, **kwargs) -> bool:
        if self._run_task is not None and not self._run_task.done():
            return False
        if self.process_state in ("starting", "running", "stopping", "closed"):
            return False

        self._stop_requested = asyncio.Event()
        self._run_exception = None
        self._last_error = None
        self._run_task = asyncio.create_task(self._run_wrapper(), name=f"{self.name or self.id}-run")
        return True

    async def _run_wrapper(self):
        final_state: ProcessStateEnum = "idle"
        try:
            if self._stop_requested.is_set():
                return
            await self.transition_to_state("starting")
            if self._stop_requested.is_set():
                return

            await self.transition_to_state("running")
            await self.transition_to_activity("processing")
            await self.run()
        except asyncio.CancelledError as e:
            if not self._stop_requested.is_set():
                final_state = "failed"
                self._record_error(e)
                logger.exception("%s run task was cancelled unexpectedly", self.name)
                raise
        except Exception as e:
            final_state = "failed"
            self._record_error(e)
            logger.exception("%s process failed", self.name)
        finally:
            try:
                await self.transition_to_activity("closing")
                await self.close_out_ports()
            except Exception as e:
                if final_state != "failed":
                    final_state = "failed"
                    self._record_error(e)
                logger.exception("%s process failed while closing output ports", self.name)
            if self.process_state != "closed":
                await self.transition_to_state(final_state)
            await self.transition_to_activity("none")
            self._stop_requested.clear()

    # stop @6 () -> (stopped :Bool);
    @override
    async def stop(self, _context, **kwargs) -> bool:
        has_running_task = self._run_task is not None and not self._run_task.done()
        if not has_running_task and self.process_state in ("idle", "failed", "closed"):
            return False

        self._stop_requested.set()
        await self.transition_to_state("stopping")
        return await self._wait_for_run_task(self.soft_stop_timeout_seconds)

    async def _wait_for_run_task(self, timeout: float) -> bool:
        if self._run_task is None:
            return True
        if self._run_task.done():
            await self._consume_run_task_result()
            return True

        done, _pending = await asyncio.wait({self._run_task}, timeout=timeout)
        if self._run_task not in done:
            logger.error(
                "%s run task did not stop within %.3fs",
                self.name,
                timeout,
            )
            return False

        await self._consume_run_task_result()
        return True

    async def _consume_run_task_result(self) -> None:
        if self._run_task is None:
            return
        try:
            await self._run_task
        except asyncio.CancelledError:
            pass

    async def run(self):
        logger.warning("run method unimplemented")

    # state @8 (transitionCallback :StateTransition) -> (currentState :State);
    @override
    async def state(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.state_transition_callbacks.append(transitionCallback)
        return self.process_state

    # activity @10 (transitionCallback :ActivityTransition) -> (currentActivity :ActivityInfo);
    @override
    async def activity(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.activity_transition_callbacks.append(transitionCallback)
        return self._activity_message()

    # lastError @9 () -> (info :ErrorInfo);
    @override
    async def lastError(self, _context, **kwargs):
        return self._last_error_message()

    @property
    def stopping(self) -> bool:
        return self._stop_requested.is_set()

    async def read_in(self, name: str) -> IPReader | IPBuilder | None:
        return await self._input_runtime.read_in(name)

    async def read_in_chunked(self, name: str) -> IPReader | IPBuilder | None:
        return await self._input_runtime.read_in_chunked(name)

    async def read_in_chunked_stream(self, name: str) -> ChunkedInputStream | None:
        return await self._input_runtime.read_in_chunked_stream(name)

    async def update_config_from_port(self, name: str = "conf") -> bool:
        in_msg = await self._input_runtime.read_in_raw(name)
        if in_msg is None:
            return False

        try:
            config_values = _config_from_ip(in_msg)
        except ProcessConfigError as e:
            raise ProcessConfigError(f"{self.name} received invalid config on port '{name}': {e}", port=name) from e

        self.apply_config_values(config_values)
        return True

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
    ) -> list[IPReader | IPBuilder] | None: ...

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
    ) -> IPReader | IPBuilder | None: ...

    async def read_array_in(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader | IPBuilder] | IPReader | IPBuilder | None:
        return await self._input_runtime.read_array_in_any(name, strategy)

    @overload
    async def read_array_in_chunked(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
    ) -> list[IPReader | IPBuilder] | None: ...

    @overload
    async def read_array_in_chunked(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
    ) -> IPReader | IPBuilder | None: ...

    async def read_array_in_chunked(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader | IPBuilder] | IPReader | IPBuilder | None:
        return await self._input_runtime.read_array_in_chunked_any(name, strategy)

    async def write_out(self, name: str, message: IPBuilder) -> bool:
        return await self._output_runtime.write_out(name, message)

    async def write_out_chunked(self, name: str, message: IPBuilder) -> bool:
        return await self._output_runtime.write_out_chunked(
            name,
            message,
            chunk_size=DEFAULT_BRACKETED_CHUNK_SIZE,
        )

    async def write_out_chunked_stream(
        self,
        name: str,
        source: IPBuilder,
        *,
        chunks: AsyncIterable[bytes],
    ) -> bool:
        return await self._output_runtime.write_out_chunked_stream(name, source, chunks=chunks)

    async def write_array_out(
        self,
        name: str,
        strategy: ArrayOutStrategy | str,
        message: IPBuilder,
    ) -> bool:
        return await self._output_runtime.write_array_out(name, strategy, message)

    async def close_in_ports(self):
        await self._input_runtime.close_in_ports()

    async def force_close_ports(self):
        await self.transition_to_activity("closing")
        await self.close_in_ports()
        await self.close_out_ports(cancel_pending_writes=True)
        await self.transition_to_activity("none")

    async def close_out_ports(self, *, cancel_pending_writes: bool | None = None):
        await self._output_runtime.close_out_ports(cancel_pending_writes=cancel_pending_writes)

    async def serve(
        self,
        writer_sr: str | None = None,
        serve_bootstrap: bool = False,
        host: str | None = None,
        port: int | None = None,
    ):
        if writer_sr and len(writer_sr) > 0 and (writer_cap := await self.con_man.try_connect(writer_sr)) is not None:
            writer = writer_cap.cast_as(fbp_capnp.Channel.Writer)
            await writer.write(value=self)
            logger.info("wrote process cap into %s", writer_sr)

        async def new_connection(stream: capnp.AsyncIoStream):
            await capnp.TwoPartyServer(stream, bootstrap=self if serve_bootstrap else None).on_disconnect()

        port = port or 0
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        try:
            async with server:
                if serve_bootstrap:
                    host = host or common.get_public_ip()
                    import socket

                    ipv4_sockets = list(filter(lambda s: s.family == socket.AddressFamily.AF_INET, server.sockets))
                    if len(ip4_socks := ipv4_sockets) > 0:
                        port = ip4_socks[0].getsockname()[1]
                    logger.info("Process(%s) SR: capnp://%s:%s", self.name, host, port)
                await server.serve_forever()
        finally:
            await self.force_close_ports()
            await self.transition_to_state("closed")
