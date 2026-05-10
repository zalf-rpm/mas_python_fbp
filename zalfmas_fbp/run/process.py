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

import argparse
import asyncio
import json
import logging
import os
import subprocess as sp
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Literal,
    TypeGuard,
    cast,
    get_args,
    get_origin,
    overload,
    override,
)

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.fbp.fbp_capnp.types.results.tuples import (
    ConnectinportResultTuple,
    ConnectoutportResultTuple,
)
from pydantic import BaseModel, ConfigDict
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from mas.schema.common.common_capnp.types.builders import (
        PairBuilder,
        ValueBuilder,
    )
    from mas.schema.common.common_capnp.types.enums import StructuredTextTypeEnum
    from mas.schema.common.common_capnp.types.readers import ValueReader
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
    from mas.schema.fbp.fbp_capnp.types.readers import ActivityInfoReader, IPReader
    from mas.schema.fbp.fbp_capnp.types.results.client import ReadResult


from zalfmas_common import common

from zalfmas_fbp.run.argparse_utils import parse_args_typed
from zalfmas_fbp.run.logging_config import (
    add_log_level_argument,
    configure_logging,
    format_exception_full,
)
from zalfmas_fbp.run.metadata import ComponentMetadata, ComponentPortMetadata

ArrayReaderPorts = list["ReaderClient | None"]
ArrayWriterPorts = list["WriterClient | None"]
ArrayOutWriteTasks = list["asyncio.Task[bool] | None"]
type ConnectedPort = ReaderClient | WriterClient
type StandardPortMap = dict[str, ReaderClient | None] | dict[str, WriterClient | None]
type ArrayPortMap = dict[str, ArrayReaderPorts] | dict[str, ArrayWriterPorts]
type ConfigScalar = str | int | float | bool
type ConfigValue = ConfigScalar | list[ConfigValue] | dict[str, ConfigValue]
type RawConfig = dict[str, ConfigValue]
DEFAULT_SOFT_STOP_TIMEOUT_SECONDS = 30.0
# Gateway-routed channel writes disconnect at ~32 MiB, so keep bracketed chunks below that limit.
DEFAULT_BRACKETED_CHUNK_SIZE = 16 * 1024 * 1024


class ProcessRuntimeError(RuntimeError):
    def __init__(self, message: str, *, phase: str, port: str | None = None):
        super().__init__(message)
        self.phase = phase
        self.port = port


class ProcessConfigError(ValueError):
    def __init__(self, message: str, *, port: str | None = None):
        super().__init__(message)
        self.phase = "config"
        self.port = port


class InputPortReadError(ProcessRuntimeError):
    def __init__(self, process_name: str | None, port: str, message: str):
        super().__init__(f"{process_name} failed reading input port '{port}': {message}", phase="read", port=port)


class OutputPortWriteError(ProcessRuntimeError):
    def __init__(self, process_name: str | None, port: str, message: str):
        super().__init__(f"{process_name} failed writing output port '{port}': {message}", phase="write", port=port)


@dataclass
class ProcessErrorInfo:
    process_id: str | None
    process_name: str | None
    phase: str
    port: str | None
    error_type: str
    message: str
    cause_type: str | None = None
    cause_message: str | None = None
    traceback: list[str] | None = None


logger = logging.getLogger(__name__)
configure_logging()


@dataclass
class ProcessArgs(argparse.Namespace):
    process_cap_writer_sr: str | None = None
    output_json_default_config: bool = False
    output_json_component_metadata: bool = False
    write_json_default_config: str | None = None
    write_json_component_metadata: str | None = None
    serve_bootstrap: bool = False
    host: str | None = None
    port: int | None = None
    name: str | None = None
    log_level: str = "WARNING"


def _is_config_list[T: ConfigScalar](
    value: list[ConfigValue],
    item_type: type[T],
    *,
    exact: bool = False,
) -> TypeGuard[list[T]]:
    if exact:
        return all(type(item) is item_type for item in value)
    return all(isinstance(item, item_type) for item in value)


def _is_config_value_list(value: object) -> TypeGuard[list[ConfigValue]]:
    return isinstance(value, list)


def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _is_str_key_dict(value: dict[object, object]) -> TypeGuard[dict[str, object]]:
    return all(isinstance(key, str) for key in value)


class ArrayInStrategy(StrEnum):
    ZIP = "zip"
    NEXT_AVAILABLE = "next_available"


class ArrayOutStrategy(StrEnum):
    BROADCAST = "broadcast"
    ROUND_ROBIN = "round_robin"
    NEXT_AVAILABLE = "next_available"


class ProcessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


ConfigT = TypeVar("ConfigT", bound=ProcessConfig | RawConfig, default=RawConfig)


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ProcessStateEnum, ProcessStateEnum],
            None,
        ],  #: Callable[[fbp_capnp.Process.State, fbp_capnp.Process.State]]
    ):
        self.callback: Callable[[ProcessStateEnum, ProcessStateEnum], None] = callback

    # stateChanged @0 (old :State, new :State);
    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class ActivityTransition(fbp_capnp.Process.ActivityTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ],
    ):
        self.callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ] = callback

    # activityChanged @0 (old :ActivityInfo, new :ActivityInfo);
    @override
    async def activityChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class PortDisconnect(fbp_capnp.Process.Disconnect.Server):
    def __init__(
        self,
        ports: StandardPortMap | ArrayPortMap,
        name: str,
        port: ConnectedPort | None,
        index: int | None = None,
    ):
        if index is None:
            self.standard_ports: StandardPortMap | None = cast("StandardPortMap", ports)
            self.array_ports: ArrayPortMap | None = None
        else:
            self.standard_ports = None
            self.array_ports = cast("ArrayPortMap", ports)
        self.name: str = name
        self.port: ConnectedPort | None = port
        self.index: int | None = index
        self.disconnected: bool = False

    @override
    async def disconnect(self, _context, **_kwargs) -> bool:
        if self.disconnected or self.port is None:
            return False

        if self._disconnect_port():
            await self._close_port()
            self.disconnected = True
            return True
        return False

    def _disconnect_port(self) -> bool:
        if self.index is None:
            return self._disconnect_standard_port()
        return self._disconnect_array_port()

    def _disconnect_standard_port(self) -> bool:
        if self.standard_ports is None or self.standard_ports.get(self.name) is not self.port:
            return False
        self.standard_ports[self.name] = None
        return True

    def _disconnect_array_port(self) -> bool:
        if self.array_ports is None or self.index is None:
            return False

        array_ports = self.array_ports.get(self.name)
        if array_ports is None:
            return False

        if self.index < len(array_ports) and array_ports[self.index] is self.port:
            array_ports[self.index] = None
            return True

        for i, port in enumerate(array_ports):
            if port is self.port:
                array_ports[i] = None
                return True
        return False

    async def _close_port(self) -> None:
        if self.port is None:
            return
        try:
            await self.port.close()
        except (capnp.KjException, RuntimeError) as e:
            logger.error("Exception closing disconnected port '%s': %s", self.name, e)


class Process(  # pyright: ignore[reportUnsafeMultipleInheritance]
    fbp_capnp.Process.Server,
    common.Identifiable,
    common.GatewayRegistrable,
    Generic[ConfigT],
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

    @staticmethod
    def _config_value_from_python(value: object) -> ValueBuilder:
        if isinstance(value, str):
            return common_capnp.Value.new_message(t=value)
        if isinstance(value, bool):
            return common_capnp.Value.new_message(b=value)
        if isinstance(value, int):
            return common_capnp.Value.new_message(i64=value)
        if isinstance(value, float):
            return common_capnp.Value.new_message(f64=value)
        if _is_config_value_list(value):
            if not value:
                return common_capnp.Value.new_message(lv=[])
            if _is_config_list(value, int, exact=True):
                return common_capnp.Value.new_message(li64=value)
            if _is_config_list(value, float):
                return common_capnp.Value.new_message(lf64=value)
            if _is_config_list(value, bool):
                return common_capnp.Value.new_message(lb=value)
            if _is_config_list(value, str):
                return common_capnp.Value.new_message(lt=value)
            values = [Process._config_value_from_python(item) for item in value]
            return common_capnp.Value.new_message(lv=values)
        if _is_object_dict(value):
            if not _is_str_key_dict(value):
                raise TypeError("Config dict keys must be strings")
            pairs: list[PairBuilder] = []
            for key, item in value.items():
                pairs.append(
                    common_capnp.Pair.new_message(
                        fst=key,
                        snd=Process._config_value_from_python(item),
                    ),
                )
            return common_capnp.Value.new_message(lpair=pairs)

        raise TypeError(f"Unsupported config value type: {type(value).__name__}")

    @staticmethod
    def _python_value_from_capnp_value(value: ValueReader) -> ConfigValue:
        value_type = value.which()
        if value_type == "i64":
            return value.i64
        if value_type == "f64":
            return value.f64
        if value_type == "b":
            return value.b
        if value_type == "t":
            return value.t
        if value_type == "li64":
            return list(value.li64)
        if value_type == "lf64":
            return list(value.lf64)
        if value_type == "lb":
            return list(value.lb)
        if value_type == "lt":
            return list(value.lt)
        if value_type == "lv":
            return [Process._python_value_from_capnp_value(v) for v in value.lv]
        if value_type == "lpair":
            try:
                d: dict[str, ConfigValue] = {}
                for pair in value.lpair:
                    if not pair._has("fst") or not pair._has("snd"):
                        raise TypeError("Pair entries must have both 'fst' and 'snd'")
                    d[pair.fst.as_text()] = Process._python_value_from_capnp_value(
                        pair.snd.as_struct(common_capnp.Value),
                    )
                return d
            except (AttributeError, capnp.KjException, TypeError) as e:
                raise TypeError(f"Error unpacking dict (list of pairs): {e}") from e
        raise TypeError(f"Unsupported config value type: {value_type}")

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

    def _apply_config_values(self, config_values: Mapping[str, ConfigValue | None]) -> None:
        self.apply_config_values(config_values)

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

    @staticmethod
    def _load_config_text(text: str, config_type: StructuredTextTypeEnum) -> dict[str, Any]:
        normalized_type = config_type or "toml"
        if isinstance(normalized_type, str):
            normalized_type = normalized_type.lower()
        if normalized_type == "toml":
            return dict(tomllib.loads(text))
        if normalized_type == "json":
            config = json.loads(text)
            if type(config) is not dict:
                raise TypeError("JSON config must decode to an object")
            return config
        raise ValueError(f"Unsupported config text type: {config_type}")

    @classmethod
    def _load_unstructured_config_text(cls, text: str) -> dict[str, Any]:
        try:
            return cls._load_config_text(text, "toml")
        except tomllib.TOMLDecodeError as toml_error:
            try:
                return cls._load_config_text(text, "json")
            except (json.JSONDecodeError, TypeError) as json_error:
                raise ValueError(
                    f"Config text is neither valid TOML nor JSON: {toml_error}; {json_error}"
                ) from json_error

    @classmethod
    def _config_from_ip(cls, in_msg: IPReader) -> dict[str, ConfigValue]:
        try:
            structured_text = in_msg.content.as_struct(common_capnp.StructuredText)
        except capnp.KjException:
            return cls._load_unstructured_config_text(in_msg.content.as_text())
        return cls._load_config_text(structured_text.value, structured_text.type)

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
            # try:
            #    self._raw_config[key] = self._config_value_from_python(value)
            # except TypeError as e:
            #    logger.warning("Ignoring unsupported default config entry '%s': %s", key, e)
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
                    val=Process._config_value_from_python(item[1]),
                ),
                self.raw_config.items(),
            ),
        )

    @override
    async def setConfigEntry(self, name, val, _context, **kwargs):
        self._apply_config_values({name: Process._python_value_from_capnp_value(val)})

    async def transition_to_state(self, new_state: ProcessStateEnum):
        if new_state == self.process_state:
            return

        prev_state = self.process_state
        self.process_state = new_state
        for cb in self.state_transition_callbacks:
            await cb.stateChanged(prev_state, self.process_state)

    def _activity_message(self) -> ActivityInfoBuilder:
        return fbp_capnp.Process.ActivityInfo.new_message(state=self.activity_state, port=self.activity_port)

    async def transition_to_activity(
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
        for cb in self.activity_transition_callbacks:
            await cb.activityChanged(old_info, new_info)

    # start @5 ();
    @override
    async def start(self, *_args: object, **_kwargs: object) -> bool:
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
    async def stop(self, *_args: object, **_kwargs: object) -> bool:
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
    async def activity(self, transitionCallback, *_args: object, **_kwargs: object):
        if transitionCallback:
            self.activity_transition_callbacks.append(transitionCallback)
        return self._activity_message()

    # lastError @9 () -> (info :ErrorInfo);
    @override
    async def lastError(self, *_args: object, **_kwargs: object):
        return self._last_error_message()

    @property
    def stopping(self) -> bool:
        return self._stop_requested.is_set()

    async def read_in(self, name: str, bracketed: bool = False) -> IPReader | IPBuilder | None:
        in_ip = await self._read_in_raw(name)
        if in_ip is None:
            return None

        if in_ip.type == "openBracket":
            if not bracketed:
                msg = f"{self.name} received a bracketed payload on input port '{name}', but bracketed reading is disabled."
                raise InputPortReadError(self.name, name, msg)
            return await self._read_bracketed_payload(name, in_ip)

        if in_ip.type == "closeBracket":
            msg = f"{self.name} received an unexpected closeBracket on input port '{name}'."
            raise InputPortReadError(self.name, name, msg)

        return in_ip

    async def _read_in_raw(self, name: str) -> IPReader | None:
        if self._stop_requested.is_set():
            return None

        port = self.in_ports.get(name)
        if port is None:
            return None

        await self.transition_to_activity("waitingInput", name)
        read_task = asyncio.ensure_future(port.read())
        stop_task = asyncio.create_task(self._stop_requested.wait())
        try:
            done, pending = await asyncio.wait(
                {read_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                _ = task.cancel()

            if stop_task in done:
                _ = read_task.cancel()
                with suppress(asyncio.CancelledError):
                    await read_task
                return None

            msg = await read_task
            await self.transition_to_activity("processing")
            if msg.which() == "done":
                self.in_ports[name] = None
                return None
            return msg.value.as_struct(fbp_capnp.IP)
        except asyncio.CancelledError:
            _ = read_task.cancel()
            _ = stop_task.cancel()
            with suppress(asyncio.CancelledError):
                await read_task
            raise
        except capnp.KjException as e:
            self.in_ports[name] = None
            if self._stop_requested.is_set():
                return None
            description = str(getattr(e, "description", e))
            logger.error("%s RPC exception reading input port '%s': %s", self.name, name, description)
            raise InputPortReadError(self.name, name, description) from e
        finally:
            if not stop_task.done():
                _ = stop_task.cancel()

    async def _read_bracketed_payload(self, name: str, open_ip: IPReader) -> IPBuilder:
        chunks: list[bytes] = []
        while True:
            chunk_ip = await self._read_in_raw(name)
            if chunk_ip is None:
                msg = f"{self.name} input port '{name}' closed before a bracketed payload ended."
                raise InputPortReadError(self.name, name, msg)
            if chunk_ip.type == "closeBracket":
                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=b"".join(chunks)))
                out_ip.attributes = list(open_ip.attributes)
                return out_ip
            if chunk_ip.type == "openBracket":
                msg = f"{self.name} received a nested openBracket on input port '{name}'."
                raise InputPortReadError(self.name, name, msg)
            chunks.append(bytes(chunk_ip.content.as_struct(common_capnp.Value).d))

    async def update_config_from_port(self, name: str = "conf") -> bool:
        in_msg = await self._read_in_raw(name)
        if in_msg is None:
            return False

        try:
            config_values = self._config_from_ip(in_msg)
        except (capnp.KjException, json.JSONDecodeError, tomllib.TOMLDecodeError, TypeError, ValueError) as e:
            raise ProcessConfigError(f"{self.name} received invalid config on port '{name}': {e}", port=name) from e

        self._apply_config_values(config_values)
        return True

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
        bracketed: bool = False,
    ) -> list[IPReader | IPBuilder] | None: ...

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
        bracketed: bool = False,
    ) -> IPReader | IPBuilder | None: ...

    async def read_array_in(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
        bracketed: bool = False,
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
                if not bracketed:
                    msg = f"{self.name} received a bracketed payload on array input port '{name}', but bracketed reading is disabled."
                    raise InputPortReadError(self.name, name, msg)
                port = ports[port_index]
                if port is None:
                    msg = (
                        f"{self.name} array input port '{name}[{port_index}]' closed before a bracketed payload ended."
                    )
                    raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
                return await self._read_bracketed_array_payload(name, port_index, port, in_ip)
            if in_ip.type == "closeBracket":
                msg = f"{self.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
            return in_ip

        read_tasks: dict[asyncio.Future[ReadResult], int] = {
            asyncio.ensure_future(port.read()): i for i, port in active_ports
        }
        stop_task = asyncio.create_task(self._stop_requested.wait())
        results: dict[int, IPReader] = {}
        zip_finished = False
        await self.transition_to_activity("waitingInput", name)

        try:
            while read_tasks:
                done, _pending = await asyncio.wait(
                    {*read_tasks, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_task in done:
                    await self._cancel_tasks(read_tasks)
                    return None

                for task in done:
                    if task is stop_task:
                        continue

                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        ports[port_index] = None
                        if self._stop_requested.is_set():
                            zip_finished = True
                            continue
                        description = str(getattr(e, "description", e))
                        logger.error(
                            "%s RPC exception reading array input port '%s[%s]': %s",
                            self.name,
                            name,
                            port_index,
                            description,
                        )
                        raise InputPortReadError(self.name, f"{name}[{port_index}]", description) from e

                    if msg.which() == "done":
                        ports[port_index] = None
                        zip_finished = True
                    else:
                        results[port_index] = msg.value.as_struct(fbp_capnp.IP)

                if zip_finished:
                    await self._cancel_tasks(read_tasks)
                    await self.transition_to_activity("processing")
                    return None

            await self.transition_to_activity("processing")
            ordered_results: list[IPReader | IPBuilder] = [results[i] for i, _port in active_ports]
            if not bracketed:
                bracketed_results = [ip for ip in ordered_results if ip.type in ("openBracket", "closeBracket")]
                if bracketed_results:
                    msg = f"{self.name} received a bracketed payload on array input port '{name}', but bracketed reading is disabled."
                    raise InputPortReadError(self.name, name, msg)
                return ordered_results
            return await self._coalesce_bracketed_array_results(name, active_ports, ports, ordered_results)
        except asyncio.CancelledError:
            await self._cancel_tasks(read_tasks)
            _ = stop_task.cancel()
            raise
        except Exception:
            await self._cancel_tasks(read_tasks)
            raise
        finally:
            if not stop_task.done():
                _ = stop_task.cancel()

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
            stop_task = asyncio.create_task(self._stop_requested.wait())
            await self.transition_to_activity("waitingInput", name)

            try:
                done, _pending = await asyncio.wait(
                    {*read_tasks, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_task in done:
                    await self._cancel_tasks(read_tasks)
                    return None

                for task in done:
                    if task is stop_task:
                        continue

                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        ports[port_index] = None
                        if self._stop_requested.is_set():
                            continue
                        description = str(getattr(e, "description", e))
                        logger.error(
                            "%s RPC exception reading array input port '%s[%s]': %s",
                            self.name,
                            name,
                            port_index,
                            description,
                        )
                        raise InputPortReadError(self.name, f"{name}[{port_index}]", description) from e

                    if msg.which() == "done":
                        ports[port_index] = None
                    else:
                        buffers[port_index] = msg.value.as_struct(fbp_capnp.IP)

                await self._cancel_tasks(read_tasks)
                if buffers:
                    port_index = next(iter(buffers))
                    return port_index, buffers.pop(port_index)

                active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
            except asyncio.CancelledError:
                await self._cancel_tasks(read_tasks)
                _ = stop_task.cancel()
                raise
            except Exception:
                await self._cancel_tasks(read_tasks)
                raise
            finally:
                if not stop_task.done():
                    _ = stop_task.cancel()

        return None

    async def _coalesce_bracketed_array_results(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
        first_messages: list[IPReader | IPBuilder],
    ) -> list[IPReader | IPBuilder]:
        coalesced: list[IPReader | IPBuilder] = []
        for (port_index, port), first_ip in zip(active_ports, first_messages, strict=True):
            if first_ip.type == "openBracket":
                coalesced.append(await self._read_bracketed_array_payload(name, port_index, port, first_ip))
            elif first_ip.type == "closeBracket":
                msg = f"{self.name} received an unexpected closeBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg)
            else:
                coalesced.append(first_ip)
        return coalesced

    async def _read_bracketed_array_payload(
        self,
        name: str,
        port_index: int,
        port: ReaderClient,
        open_ip: IPReader | IPBuilder,
    ) -> IPBuilder:
        chunks: list[bytes] = []
        while True:
            try:
                await self.transition_to_activity("waitingInput", f"{name}[{port_index}]")
                msg = await port.read()
                await self.transition_to_activity("processing")
            except capnp.KjException as e:
                ports = self.array_in_ports.get(name)
                if ports is not None and port_index < len(ports):
                    ports[port_index] = None
                description = str(getattr(e, "description", e))
                raise InputPortReadError(self.name, f"{name}[{port_index}]", description) from e

            if msg.which() == "done":
                ports = self.array_in_ports.get(name)
                if ports is not None and port_index < len(ports):
                    ports[port_index] = None
                msg_text = (
                    f"{self.name} array input port '{name}[{port_index}]' closed before a bracketed payload ended."
                )
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg_text)

            chunk_ip = msg.value.as_struct(fbp_capnp.IP)
            if chunk_ip.type == "closeBracket":
                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=b"".join(chunks)))
                out_ip.attributes = list(open_ip.attributes)
                return out_ip
            if chunk_ip.type == "openBracket":
                msg_text = f"{self.name} received a nested openBracket on array input port '{name}[{port_index}]'."
                raise InputPortReadError(self.name, f"{name}[{port_index}]", msg_text)
            chunks.append(bytes(chunk_ip.content.as_struct(common_capnp.Value).d))

    @staticmethod
    async def _cancel_tasks(tasks: Iterable[asyncio.Future[Any]]) -> None:
        pending = list(tasks)
        for task in pending:
            _ = task.cancel()
        _ = await asyncio.gather(*pending, return_exceptions=True)

    async def write_out(self, name: str, message: IPBuilder, bracketed: bool = False) -> bool:
        if bracketed:
            return await self._write_out_bracketed(name, message)
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
            description = str(getattr(e, "description", e))
            logger.error("%s RPC exception writing output port '%s': %s", self.name, name, description)
            raise OutputPortWriteError(self.name, name, description) from e

    async def _write_out_bracketed(self, name: str, message: IPBuilder) -> bool:
        if message.type != "standard":
            msg = f"{self.name} can only write standard IPs as bracketed payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg)

        try:
            data = bytes(message.content.as_struct(common_capnp.Value).d)
        except (capnp.KjException, TypeError) as e:
            msg = f"{self.name} can only bracket common.capnp:Value[Data] payloads on output port '{name}'."
            raise OutputPortWriteError(self.name, name, msg) from e

        port = self.out_ports.get(name)
        if port is None:
            return False

        open_ip = self._bracket_ip("openBracket", message)
        close_ip = self._bracket_ip("closeBracket", message)
        open_sent = False
        close_sent = False
        aborted = False
        rpc_error: capnp.KjException | None = None
        description: str | None = None

        try:
            await port.write(value=open_ip)
            open_sent = True
            for offset in range(0, len(data), DEFAULT_BRACKETED_CHUNK_SIZE):
                if self._stop_requested.is_set():
                    aborted = True
                    break
                chunk_ip = fbp_capnp.IP.new_message(
                    content=common_capnp.Value.new_message(d=data[offset : offset + DEFAULT_BRACKETED_CHUNK_SIZE]),
                )
                chunk_ip.attributes = list(message.attributes)
                await port.write(value=chunk_ip)

            if not aborted:
                await port.write(value=close_ip)
                close_sent = True
        except capnp.KjException as e:
            rpc_error = e
            description = str(getattr(e, "description", e))
        finally:
            if open_sent and not close_sent:
                with suppress(capnp.KjException, RuntimeError):
                    await port.write(value=close_ip)
                    close_sent = True

            if rpc_error is not None and self.out_ports.get(name) is port:
                self.out_ports[name] = None

        if rpc_error is not None:
            if self._stop_requested.is_set():
                return False
            logger.error("%s RPC exception writing output port '%s': %s", self.name, name, description)
            raise OutputPortWriteError(self.name, name, description or "") from rpc_error

        return not aborted

    @staticmethod
    def _bracket_ip(bracket_type: Literal["openBracket", "closeBracket"], source: IPBuilder | IPReader) -> IPBuilder:
        bracket_ip = fbp_capnp.IP.new_message(type=bracket_type)
        bracket_ip.attributes = list(source.attributes)
        return bracket_ip

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

            stop_task = asyncio.create_task(self._stop_requested.wait())
            try:
                await self.transition_to_activity("waitingOutput", name)
                done, _pending = await asyncio.wait(
                    {*active_tasks, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    return None
                await self.transition_to_activity("processing")
                for task in done:
                    if task is stop_task:
                        continue
                    write_task = cast("asyncio.Task[bool]", task)
                    _ = await self._consume_array_out_write_task(name, active_tasks[write_task])
            finally:
                if not stop_task.done():
                    stop_task.cancel()

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
            description = str(getattr(e, "description", e))
            logger.error(
                "%s RPC exception writing array output port '%s[%s]': %s",
                self.name,
                name,
                port_index,
                description,
            )
            raise OutputPortWriteError(self.name, f"{name}[{port_index}]", description) from e

    async def close_in_ports(self):
        for name, port in self.in_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.in_ports[name] = None
                    logger.info("closed in port '%s'", name)
                except (capnp.KjException, RuntimeError) as e:
                    logger.error("%s: Exception closing in port '%s': %s", os.path.basename(__file__), name, e)
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
                    logger.error("%s: Exception closing out port '%s': %s", os.path.basename(__file__), name, e)
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


def start_local_process_component(
    path_to_executable: str,
    process_cap_writer_sr: str,
    name: str | None = None,
    log_level: str | None = None,
) -> sp.Popen[str]:
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    proc = sp.Popen(
        pte_split
        + [process_cap_writer_sr]
        + ([f'--name="{name}"'] if name else [])
        + ([f"--log_level={log_level}"] if log_level else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc


def create_default_args_parser(
    component_description: str,
):
    parser = argparse.ArgumentParser(description=component_description)
    _ = parser.add_argument(
        "process_cap_writer_sr",
        type=str,
        nargs="?",
        help="SturdyRef to the Writer[fbp.capnp:Process]. Writes process capability on startup to writer.",
    )
    _ = parser.add_argument(
        "--output_json_default_config",
        "-o",
        action="store_true",
        help="Output JSON configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--output_json_component_metadata",
        "-O",
        action="store_true",
        help="Output JSON component metadata at commandline. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "--write_json_default_config",
        "-w",
        type=str,
        help="Output JSON configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--write_json_component_metadata",
        "-W",
        type=str,
        help="Output JSON component metadata in the current directory. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "-b",
        "--serve_bootstrap",
        action="store_true",
        help="Serve process as the bootstrap object.>",
    )
    _ = parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    add_log_level_argument(parser)
    return parser


def run_process_from_metadata_and_cmd_args(
    p: Process[Any],
    component_meta: ComponentMetadata,
):
    parser = create_default_args_parser(component_description=p.description)
    args = parse_args_typed(parser, ProcessArgs)
    configure_logging(args.log_level)
    metadata = component_meta
    default_config = metadata.default_config_values()
    metadata_json = metadata.model_dump(mode="json", exclude_none=True)
    if args.name is not None:
        p.name = args.name
    if args.output_json_default_config:
        _ = sys.stdout.write(json.dumps(default_config, indent=4) + "\n")
        exit(0)
    elif args.write_json_default_config:
        with open(args.write_json_default_config, "w") as _:
            json.dump(default_config, _, indent=4)
            exit(0)
    elif args.output_json_component_metadata:
        _ = sys.stdout.write(json.dumps(metadata_json, indent=4) + "\n")
        exit(0)
    elif args.write_json_component_metadata:
        with open(args.write_json_component_metadata, "w") as _:
            json.dump(metadata_json, _, indent=4)
            exit(0)
    if args.process_cap_writer_sr:
        asyncio.run(
            capnp.run(
                p.serve(
                    writer_sr=args.process_cap_writer_sr,
                    serve_bootstrap=args.serve_bootstrap,
                    host=args.host,
                    port=args.port,
                ),
            ),
        )
    else:
        logger.error("A sturdy ref to a writer capability is necessary to start the process.")
