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
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from pathlib import Path
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
    from mas.schema.fbp.fbp_capnp.types.results.client import ReadResult


from zalfmas_common import common

from zalfmas_fbp.run.logging_config import (
    configure_logging,
    format_exception_full,
)
from zalfmas_fbp.run.metadata import ComponentMetadata, ComponentPortMetadata

from .chunked_io import (
    DEFAULT_BRACKETED_CHUNK_SIZE,
    ChunkedInputStream,
)
from .chunked_io import bracket_ip as _bracket_ip
from .chunked_io import chunked_blob_ip as _chunked_blob_ip
from .chunked_io import ip_blob_payload as _ip_blob_payload
from .chunked_io import ip_content_type as _ip_content_type
from .config_codec import (
    config_from_ip as _config_from_ip,
)
from .config_codec import (
    config_value_from_python,
    python_value_from_capnp_value,
)
from .errors import (
    InputPortReadError,
    OutputPortWriteError,
    ProcessConfigError,
    ProcessErrorInfo,
)
from .io_runtime import (
    cancel_tasks,
    kj_exception_description,
    wait_for_tasks_or_stop,
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
        self.in_ports: dict[str, ReaderClient | None] = {}
        self.array_in_ports: dict[str, ArrayReaderPorts] = {}
        self._array_in_buffers: dict[str, dict[int, IPReader]] = {}
        self.out_ports: dict[str, WriterClient | None] = {}
        self.array_out_ports: dict[str, ArrayWriterPorts] = {}
        self._array_out_next_indices: dict[str, int] = {}
        self._array_out_write_tasks: dict[str, ArrayOutWriteTasks] = {}
        self._run_task: asyncio.Task[None] | None = None
        self._run_exception: BaseException | None = None
        self._last_error: ProcessErrorInfo | None = None
        self._stop_requested: asyncio.Event = asyncio.Event()
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
            self._ensure_array_out_write_task_slots(name, self.array_out_ports[name])
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
        in_ip = await self._read_in_raw(name)
        if in_ip is None:
            return None

        if in_ip.type == "openBracket":
            msg = f"{self.name} received a chunked payload on input port '{name}'; use read_in_chunked* instead."
            raise InputPortReadError(self.name, name, msg)

        if in_ip.type == "closeBracket":
            msg = f"{self.name} received an unexpected closeBracket on input port '{name}'."
            raise InputPortReadError(self.name, name, msg)

        return in_ip

    async def read_in_chunked(self, name: str) -> IPReader | IPBuilder | None:
        in_ip = await self._read_in_raw(name)
        if in_ip is None:
            return None
        return await self._coalesce_chunked_input(name, name, in_ip, lambda: self._read_in_raw(name))

    async def read_in_chunked_stream(self, name: str) -> ChunkedInputStream | None:
        in_ip = await self._read_in_raw(name)
        if in_ip is None:
            return None
        return await self._chunked_stream_for_ip(name, name, in_ip, lambda: self._read_in_raw(name))

    def _clear_in_port(self, name: str) -> None:
        self.in_ports[name] = None

    def _clear_array_in_port(self, name: str, port_index: int) -> None:
        ports = self.array_in_ports.get(name)
        if ports is not None and port_index < len(ports):
            ports[port_index] = None

    def _input_port_rpc_error(self, port_label: str, error: capnp.KjException) -> InputPortReadError:
        description = kj_exception_description(error)
        logger.error("%s RPC exception reading input port '%s': %s", self.name, port_label, description)
        return InputPortReadError(self.name, port_label, description)

    def _output_port_rpc_error(self, port_label: str, error: capnp.KjException) -> OutputPortWriteError:
        description = kj_exception_description(error)
        logger.error("%s RPC exception writing output port '%s': %s", self.name, port_label, description)
        return OutputPortWriteError(self.name, port_label, description)

    async def _read_connected_port(
        self,
        *,
        port: ReaderClient,
        port_label: str,
        on_disconnect: Callable[[], None],
    ) -> IPReader | None:
        if self._stop_requested.is_set():
            return None

        await self.transition_to_activity("waitingInput", port_label)
        read_task = asyncio.ensure_future(port.read())
        try:
            done_tasks, stopped = await wait_for_tasks_or_stop({read_task}, self._stop_requested)
            if stopped:
                if read_task not in done_tasks:
                    await cancel_tasks((read_task,))
                    return None
                _ = await read_task
                return None

            msg = await read_task
            await self.transition_to_activity("processing")
            if msg.which() == "done":
                on_disconnect()
                return None
            return msg.value.as_struct(fbp_capnp.IP)
        except asyncio.CancelledError:
            await cancel_tasks((read_task,))
            raise
        except capnp.KjException as e:
            on_disconnect()
            if self._stop_requested.is_set():
                return None
            raise self._input_port_rpc_error(port_label, e) from e

    async def _read_in_raw(self, name: str) -> IPReader | None:
        port = self.in_ports.get(name)
        if port is None:
            return None
        return await self._read_connected_port(
            port=port,
            port_label=name,
            on_disconnect=lambda: self._clear_in_port(name),
        )

    async def update_config_from_port(self, name: str = "conf") -> bool:
        in_msg = await self._read_in_raw(name)
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
        strategy = ArrayInStrategy(strategy)
        if self._stop_requested.is_set():
            return None

        ports = self.array_in_ports.get(name)
        if not ports:
            return None

        active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
        if not active_ports:
            return None

        if strategy == ArrayInStrategy.NEXT_AVAILABLE:
            next_result = await self._read_array_in_next_available(name, active_ports, ports)
            if next_result is None:
                return None

            port_index, in_ip = next_result
            await self.transition_to_activity("processing")
            if in_ip.type == "openBracket":
                msg = (
                    f"{self.name} received a chunked payload on array input port '{name}[{port_index}]'; "
                    "use read_array_in_chunked instead."
                )
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
            if in_ip.type == "closeBracket":
                msg = f"{self.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
            return in_ip

        ordered_results = await self._read_array_zip_raw(name, active_ports, ports)
        if ordered_results is None:
            return None

        bracketed_results = [ip for ip in ordered_results if ip.type in ("openBracket", "closeBracket")]
        if bracketed_results:
            msg = f"{self.name} received a chunked payload on array input port '{name}'; use read_array_in_chunked."
            raise InputPortReadError(self.name, name, msg)
        return ordered_results

    async def _read_array_in_next_available(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
    ) -> tuple[int, IPReader] | None:
        buffers = self._array_in_buffers.setdefault(name, {})
        if buffers:
            port_index = next(iter(buffers))
            return port_index, buffers.pop(port_index)

        while active_ports:
            read_tasks: dict[asyncio.Future[ReadResult], int] = {
                asyncio.ensure_future(port.read()): i for i, port in active_ports
            }
            await self.transition_to_activity("waitingInput", name)

            try:
                done_tasks, stopped = await wait_for_tasks_or_stop(read_tasks, self._stop_requested)
                if stopped:
                    await cancel_tasks(read_tasks)
                    return None

                for task in done_tasks:
                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        ports[port_index] = None
                        if self._stop_requested.is_set():
                            continue
                        raise self._input_port_rpc_error(f"{name}[{port_index}]", e) from e

                    if msg.which() == "done":
                        ports[port_index] = None
                    else:
                        buffers[port_index] = msg.value.as_struct(fbp_capnp.IP)

                await cancel_tasks(read_tasks)
                if buffers:
                    port_index = next(iter(buffers))
                    return port_index, buffers.pop(port_index)

                active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
            except asyncio.CancelledError:
                await cancel_tasks(read_tasks)
                raise
            except Exception:
                await cancel_tasks(read_tasks)
                raise

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
        strategy = ArrayInStrategy(strategy)
        if self._stop_requested.is_set():
            return None

        ports = self.array_in_ports.get(name)
        if not ports:
            return None

        active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
        if not active_ports:
            return None

        if strategy == ArrayInStrategy.NEXT_AVAILABLE:
            next_result = await self._read_array_in_next_available(name, active_ports, ports)
            if next_result is None:
                return None

            port_index, in_ip = next_result
            await self.transition_to_activity("processing")
            port = ports[port_index]
            read_next_ip = (
                (lambda: self._read_array_port_raw(name, port_index, port))
                if in_ip.type == "openBracket" and port is not None
                else (lambda: self._read_in_raw(name))
            )
            return await self._coalesce_chunked_input(name, f"{name}[{port_index}]", in_ip, read_next_ip)

        ordered_results = await self._read_array_zip_raw(name, active_ports, ports)
        if ordered_results is None:
            return None
        return await self._coalesce_chunked_array_results(name, active_ports, ports, ordered_results)

    async def _read_array_zip_raw(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
    ) -> list[IPReader | IPBuilder] | None:
        read_tasks: dict[asyncio.Future[ReadResult], int] = {
            asyncio.ensure_future(port.read()): i for i, port in active_ports
        }
        results: dict[int, IPReader] = {}
        zip_finished = False
        await self.transition_to_activity("waitingInput", name)

        try:
            while read_tasks:
                done_tasks, stopped = await wait_for_tasks_or_stop(read_tasks, self._stop_requested)
                if stopped:
                    await cancel_tasks(read_tasks)
                    return None

                for task in done_tasks:
                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        ports[port_index] = None
                        if self._stop_requested.is_set():
                            zip_finished = True
                            continue
                        raise self._input_port_rpc_error(f"{name}[{port_index}]", e) from e

                    if msg.which() == "done":
                        ports[port_index] = None
                        zip_finished = True
                    else:
                        results[port_index] = msg.value.as_struct(fbp_capnp.IP)

                if zip_finished:
                    await cancel_tasks(read_tasks)
                    await self.transition_to_activity("processing")
                    return None

            await self.transition_to_activity("processing")
            return cast("list[IPReader | IPBuilder]", [results[i] for i, _port in active_ports])
        except asyncio.CancelledError:
            await cancel_tasks(read_tasks)
            raise
        except Exception:
            await cancel_tasks(read_tasks)
            raise

    async def _coalesce_chunked_array_results(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
        first_messages: list[IPReader | IPBuilder],
    ) -> list[IPReader | IPBuilder]:
        coalesced: list[IPReader | IPBuilder] = []
        for (port_index, port), first_ip in zip(active_ports, first_messages, strict=True):
            if first_ip.type == "openBracket":
                coalesced.append(
                    await self._coalesce_chunked_input(
                        name,
                        f"{name}[{port_index}]",
                        first_ip,
                        lambda port=port, port_index=port_index: self._read_array_port_raw(name, port_index, port),
                    )
                )
            elif first_ip.type == "closeBracket":
                msg = f"{self.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
            else:
                coalesced.append(first_ip)
        return coalesced

    async def _read_array_port_raw(
        self,
        name: str,
        port_index: int,
        port: ReaderClient,
    ) -> IPReader | None:
        return await self._read_connected_port(
            port=port,
            port_label=f"{name}[{port_index}]",
            on_disconnect=lambda: self._clear_array_in_port(name, port_index),
        )

    async def _coalesce_chunked_input(
        self,
        name: str,
        port_label: str,
        in_ip: IPReader | IPBuilder,
        read_next_ip: Callable[[], Awaitable[IPReader | None]],
    ) -> IPReader | IPBuilder:
        if in_ip.type == "openBracket":
            stream = await self._chunked_stream_for_ip(name, port_label, in_ip, read_next_ip)
            return await stream.collect_blob()
        if in_ip.type == "closeBracket":
            msg = f"{self.name} received an unexpected closeBracket on input port '{port_label}'."
            raise InputPortReadError(self.name, port_label, msg)
        return in_ip

    async def _chunked_stream_for_ip(
        self,
        name: str,
        port_label: str,
        in_ip: IPReader | IPBuilder,
        read_next_ip: Callable[[], Awaitable[IPReader | None]],
    ) -> ChunkedInputStream:
        if in_ip.type == "closeBracket":
            msg = f"{self.name} received an unexpected closeBracket on input port '{port_label}'."
            raise InputPortReadError(self.name, port_label, msg)

        if in_ip.type == "openBracket":
            return ChunkedInputStream(
                open_ip=in_ip,
                process_name=self.name,
                port=port_label,
                _read_next_ip=read_next_ip,
                _is_stopping=self._stop_requested.is_set,
                _on_complete=lambda: self.transition_to_activity("processing", delay_processing=False),
                content_type=_ip_content_type(in_ip),
            )

        try:
            data, content_type = _ip_blob_payload(in_ip)
        except (capnp.KjException, TypeError) as e:
            msg = f"{self.name} can only read common.capnp:Blob chunked payloads on input port '{port_label}'."
            raise InputPortReadError(self.name, port_label, msg) from e

        return ChunkedInputStream(
            open_ip=in_ip,
            process_name=self.name,
            port=port_label,
            _read_next_ip=lambda: asyncio.sleep(0, result=None),
            _is_stopping=self._stop_requested.is_set,
            _on_complete=None,
            _single_chunk=data,
            content_type=content_type,
        )

    async def write_out(self, name: str, message: IPBuilder) -> bool:
        return await self._write_out_single(name, message)

    async def _write_out_single(self, name: str, message: IPBuilder) -> bool:
        if self._stop_requested.is_set():
            return False

        port = self.out_ports.get(name)
        if port is None:
            return False

        try:
            await self.transition_to_activity("waitingOutput", name)
            await port.write(value=message)
            await self.transition_to_activity("processing")
            return True
        except capnp.KjException as e:
            self.out_ports[name] = None
            if self._stop_requested.is_set():
                return False
            raise self._output_port_rpc_error(name, e) from e

    async def write_out_chunked(self, name: str, message: IPBuilder) -> bool:
        if message.type != "standard":
            msg = f"{self.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg)

        try:
            data, content_type = _ip_blob_payload(message)
        except (capnp.KjException, TypeError) as e:
            msg = f"{self.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg) from e

        chunk_size = DEFAULT_BRACKETED_CHUNK_SIZE
        chunk_count = (len(data) + chunk_size - 1) // chunk_size

        async def data_chunks() -> AsyncIterator[bytes]:
            for offset in range(0, len(data), chunk_size):
                yield data[offset : offset + chunk_size]

        return await self._write_out_chunked_stream(
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
    ) -> bool:
        if source.type != "standard":
            msg = f"{self.name} can only write standard IPs as chunked payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg)

        try:
            _data, content_type = _ip_blob_payload(source)
        except (capnp.KjException, TypeError) as e:
            msg = f"{self.name} can only chunk common.capnp:Blob payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg) from e

        return await self._write_out_chunked_stream(
            name,
            source,
            chunks=chunks,
            content_type=content_type,
        )

    async def _write_out_chunked_stream(
        self,
        name: str,
        source: IPBuilder,
        *,
        chunks: AsyncIterable[bytes],
        content_type: str | None,
        chunk_count: int = 0,
    ) -> bool:
        port = self.out_ports.get(name)
        if port is None:
            return False

        open_ip = _bracket_ip("openBracket", source, content_type=content_type, chunk_count=chunk_count)
        close_ip = _bracket_ip("closeBracket", source, content_type=content_type, chunk_count=chunk_count)
        open_sent = False
        close_sent = False
        aborted = False
        rpc_error: capnp.KjException | None = None
        chunk_iterator = chunks.__aiter__()
        aclose = cast("Callable[[], Awaitable[object]] | None", getattr(chunk_iterator, "aclose", None))

        try:
            await self.transition_to_activity("waitingOutput", name)
            await port.write(value=open_ip)
            open_sent = True
            async for chunk in chunk_iterator:
                if self._stop_requested.is_set():
                    aborted = True
                    break
                chunk_ip = _chunked_blob_ip(chunk, content_type=content_type)
                await port.write(value=chunk_ip)

            if not aborted:
                await port.write(value=close_ip)
                close_sent = True
            await self.transition_to_activity("processing")
        except capnp.KjException as e:
            rpc_error = e
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
                except RuntimeError as e:
                    logger.warning("%s chunk iterator cleanup failed on output port '%s': %s", self.name, name, e)

        if rpc_error is not None:
            if self._stop_requested.is_set():
                return False
            raise self._output_port_rpc_error(name, rpc_error) from rpc_error

        return not aborted

    async def write_array_out(
        self,
        name: str,
        strategy: ArrayOutStrategy | str,
        message: IPBuilder,
    ) -> bool:
        if self._stop_requested.is_set():
            return False

        ports = self.array_out_ports.get(name)
        if not ports:
            return False

        strategy = ArrayOutStrategy(strategy)
        if strategy == ArrayOutStrategy.BROADCAST:
            active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
            if not active_ports:
                return False

            write_tasks = [
                asyncio.create_task(
                    self._write_array_out_port(name, i, port, message),
                    name=f"{self.name or self.id}-{name}[{i}]-broadcast-write",
                )
                for i, port in active_ports
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
            return await self._write_array_out_next_available(name, ports, message)

        start_index = self._array_out_next_indices.get(name, 0)
        for offset in range(len(ports)):
            port_index = (start_index + offset) % len(ports)
            port = ports[port_index]
            if port is None:
                continue

            self._array_out_next_indices[name] = (port_index + 1) % len(ports)
            if await self._write_array_out_port(name, port_index, port, message):
                return True

        return False

    def _ensure_array_out_write_task_slots(
        self,
        name: str,
        ports: ArrayWriterPorts,
    ) -> ArrayOutWriteTasks:
        tasks = self._array_out_write_tasks.setdefault(name, [])
        if len(tasks) < len(ports):
            tasks.extend([None] * (len(ports) - len(tasks)))
        return tasks

    async def _consume_array_out_write_task(self, name: str, port_index: int) -> bool:
        tasks = self._array_out_write_tasks.get(name)
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

    async def _wait_for_next_available_array_out_port(
        self,
        name: str,
        ports: ArrayWriterPorts,
    ) -> tuple[int, WriterClient] | None:
        tasks = self._ensure_array_out_write_task_slots(name, ports)
        while not self._stop_requested.is_set():
            for port_index, task in enumerate(tasks[: len(ports)]):
                if task is not None and task.done():
                    _ = await self._consume_array_out_write_task(name, port_index)

            active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
            if not active_ports:
                return None

            start_index = self._array_out_next_indices.get(name, 0)
            for offset in range(len(ports)):
                port_index = (start_index + offset) % len(ports)
                port = ports[port_index]
                if port is None:
                    continue
                if tasks[port_index] is None:
                    self._array_out_next_indices[name] = (port_index + 1) % len(ports)
                    return port_index, port

            active_tasks: dict[asyncio.Task[bool], int] = {
                cast("asyncio.Task[bool]", tasks[i]): i for i, _port in active_ports if tasks[i] is not None
            }
            if not active_tasks:
                return None

            await self.transition_to_activity("waitingOutput", name)
            done_tasks, stopped = await wait_for_tasks_or_stop(active_tasks, self._stop_requested)
            if stopped:
                return None
            await self.transition_to_activity("processing")
            for task in done_tasks:
                write_task = cast("asyncio.Task[bool]", task)
                _ = await self._consume_array_out_write_task(name, active_tasks[write_task])

        return None

    async def _write_array_out_next_available(
        self,
        name: str,
        ports: ArrayWriterPorts,
        message: IPBuilder,
    ) -> bool:
        next_port = await self._wait_for_next_available_array_out_port(name, ports)
        if next_port is None:
            return False

        port_index, port = next_port
        tasks = self._ensure_array_out_write_task_slots(name, ports)
        tasks[port_index] = asyncio.create_task(
            self._write_array_out_port(name, port_index, port, message, track_activity=False),
            name=f"{self.name or self.id}-{name}[{port_index}]-write",
        )
        return True

    async def _write_array_out_port(
        self,
        name: str,
        port_index: int,
        port: WriterClient,
        message: IPBuilder,
        track_activity: bool = True,
    ) -> bool:
        try:
            if track_activity:
                await self.transition_to_activity("waitingOutput", f"{name}[{port_index}]")
            await port.write(value=message)
            if track_activity:
                await self.transition_to_activity("processing")
            return True
        except capnp.KjException as e:
            self.array_out_ports[name][port_index] = None
            if self._stop_requested.is_set():
                return False
            raise self._output_port_rpc_error(f"{name}[{port_index}]", e) from e

    async def close_in_ports(self):
        for name, port in self.in_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.in_ports[name] = None
                    logger.info("closed in port '%s'", name)
                except (capnp.KjException, RuntimeError) as e:
                    logger.error("%s: Exception closing in port '%s': %s", Path(__file__).name, name, e)
        for name, ports in self.array_in_ports.items():
            for i, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[i] = None
                        logger.info("closed array in port '%s[%s]'", name, i)
                    except (capnp.KjException, RuntimeError) as e:
                        logger.error("Exception closing array in port '%s[%s]': %s", name, i, e)

    async def force_close_ports(self):
        await self.transition_to_activity("closing")
        await self.close_in_ports()
        await self.close_out_ports(cancel_pending_writes=True)
        await self.transition_to_activity("none")

    async def _finalize_array_out_write_tasks(self, *, cancel_pending: bool) -> None:
        task_refs: list[tuple[str, int, asyncio.Task[bool]]] = []
        for name, tasks in self._array_out_write_tasks.items():
            for port_index, task in enumerate(tasks):
                if task is None:
                    continue
                if task.done():
                    _ = await self._consume_array_out_write_task(name, port_index)
                    continue
                if cancel_pending:
                    _ = task.cancel()
                task_refs.append((name, port_index, task))

        if task_refs:
            _ = await asyncio.gather(*(task for _name, _port_index, task in task_refs), return_exceptions=True)
            for name, port_index, _task in task_refs:
                _ = await self._consume_array_out_write_task(name, port_index)

    async def close_out_ports(self, *, cancel_pending_writes: bool | None = None):
        if cancel_pending_writes is None:
            cancel_pending_writes = self._stop_requested.is_set()
        await self._finalize_array_out_write_tasks(cancel_pending=cancel_pending_writes)

        for name, port in self.out_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.out_ports[name] = None
                    logger.info("closed out port '%s'", name)
                except (capnp.KjException, RuntimeError) as e:
                    logger.error("%s: Exception closing out port '%s': %s", Path(__file__).name, name, e)
        for name, ports in self.array_out_ports.items():
            for i, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[i] = None
                        logger.info("closed array out port '%s[%s]'", name, i)
                    except (capnp.KjException, RuntimeError) as e:
                        logger.error("Exception closing array out port '%s[%s]': %s", name, i, e)

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
