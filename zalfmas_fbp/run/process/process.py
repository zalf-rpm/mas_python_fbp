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

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import (
        ReaderClient,
        WriterClient,
    )
    from mas.schema.fbp.fbp_capnp.types.enums import (
        ProcessActivityStateEnum,
        ProcessRunInfoOutcomeEnum,
        ProcessRunInfoPhaseEnum,
        ProcessStateEnum,
    )
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader


from zalfmas_common import common

from zalfmas_fbp.run.logging_config import (
    configure_logging,
)
from zalfmas_fbp.run.metadata import ComponentMetadata

from .bootstrap import ProcessBootstrap
from .config.config_codec import (
    config_from_ip as _config_from_ip,
)
from .config.config_codec import (
    config_value_from_python,
    python_value_from_capnp_value,
)
from .config.config_runtime import ProcessConfigRuntime
from .context import (
    ProcessContext,
)
from .errors import (
    ProcessConfigError,
)
from .io.chunked_io import DEFAULT_BRACKETED_CHUNK_SIZE, ChunkedInputStream
from .runtime.input_runtime import InputRuntime
from .runtime.lifecycle_runtime import ProcessLifecycleRuntime
from .runtime.output_runtime import OutputRuntime
from .runtime.port_runtime import ProcessPortRuntime
from .runtime.state_runtime import ProcessStateRuntime
from .types import (
    ArrayInStrategy,
    ArrayOutStrategy,
    ArrayReaderPorts,
    ArrayWriterPorts,
    ConfigValue,
    ProcessConfig,
    RawConfig,
)

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

        metadata_config_model = self._metadata_config_model(metadata)
        if (
            self.config_model is not None
            and metadata_config_model is not None
            and self.config_model is not metadata_config_model
        ):
            msg = (
                f"{type(self).__name__} defines config model {self.config_model.__name__}, "
                f"but metadata specifies {metadata_config_model.__name__}."
            )
            raise TypeError(msg)
        resolved_config_model = self.config_model or metadata_config_model

        self._context: ProcessContext = ProcessContext(metadata=metadata)
        self._config_runtime: ProcessConfigRuntime[ConfigT] = ProcessConfigRuntime(
            state=self._context.config,
            config_model=resolved_config_model,
        )
        self._bootstrap: ProcessBootstrap[ConfigT] = ProcessBootstrap(
            metadata=self._context.metadata,
            ports=self._context.ports,
            config_runtime=self._config_runtime,
            identity=self,
        )
        self._state_runtime: ProcessStateRuntime = ProcessStateRuntime(
            identity=self,
            state=self._context.status,
            processing_delay_milliseconds=DEFAULT_PROCESSING_ACTIVITY_DELAY_MILLISECONDS,
        )
        self._input_runtime: InputRuntime = InputRuntime(
            identity=self,
            ports=self._context.ports,
            stop_event=self._context.lifecycle.stop_requested,
            activity=self._state_runtime,
        )
        self._output_runtime: OutputRuntime = OutputRuntime(
            identity=self,
            ports=self._context.ports,
            stop_event=self._context.lifecycle.stop_requested,
            activity=self._state_runtime,
        )
        self._port_runtime: ProcessPortRuntime = ProcessPortRuntime(
            metadata=self._context.metadata,
            ports=self._context.ports,
            con_man=self.con_man,
            output_runtime=self._output_runtime,
        )
        self._lifecycle_runtime: ProcessLifecycleRuntime = ProcessLifecycleRuntime(
            identity=self,
            lifecycle=self._context.lifecycle,
            status=self._context.status,
            state_runtime=self._state_runtime,
            input_runtime=self._input_runtime,
            output_runtime=self._output_runtime,
            run_fn=self.run,
        )
        self._context.lifecycle.soft_stop_timeout_seconds = DEFAULT_SOFT_STOP_TIMEOUT_SECONDS

        self._bootstrap.initialize_from_metadata()

    @staticmethod
    def _metadata_config_model(metadata: ComponentMetadata | None) -> type[ProcessConfig] | None:
        if metadata is None or metadata.config is None:
            return None
        if not issubclass(metadata.config, ProcessConfig):
            msg = f"Metadata config model must inherit from ProcessConfig, got {metadata.config.__name__}."
            raise TypeError(msg)
        return metadata.config

    @property
    def context(self) -> ProcessContext:
        return self._context

    def apply_config_values(self, config_values: Mapping[str, ConfigValue | None]) -> None:
        self._config_runtime.apply_config_values(config_values)

    @staticmethod
    def _schema_run_outcome(outcome: str) -> ProcessRunInfoOutcomeEnum:
        match outcome:
            case "completed":
                return "completed"
            case "stopped":
                return "stopped"
            case "failed":
                return "failed"
            case _:
                return "none"

    @staticmethod
    def _schema_run_phase(phase: str) -> ProcessRunInfoPhaseEnum:
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

    def _last_run_message(self):
        last_run = self.context.lifecycle.last_run
        if last_run is None:
            return fbp_capnp.Process.RunInfo.new_message(hasRunInfo=False)

        return fbp_capnp.Process.RunInfo.new_message(
            hasRunInfo=True,
            processId=last_run.process_id or "",
            processName=last_run.process_name or "",
            outcome=self._schema_run_outcome(last_run.outcome),
            phase=self._schema_run_phase(last_run.phase),
            port=last_run.port or "",
            detailType=last_run.detail_type,
            message=last_run.message,
            causeType=last_run.cause_type or "",
            causeMessage=last_run.cause_message or "",
            traceback=last_run.traceback or [],
        )

    @property
    def config(self) -> ConfigT:
        return cast("ConfigT", self.context.config.config)

    @property
    def raw_config(self) -> RawConfig:
        return self.context.config.raw_config

    @property
    def stop_event(self) -> asyncio.Event:
        return self.context.lifecycle.stop_requested

    @property
    def in_ports(self) -> dict[str, ReaderClient | None]:
        return self._input_runtime.in_ports

    @property
    def array_in_ports(self) -> dict[str, ArrayReaderPorts]:
        return self._input_runtime.array_in_ports

    @property
    def out_ports(self) -> dict[str, WriterClient | None]:
        return self._output_runtime.out_ports

    @property
    def array_out_ports(self) -> dict[str, ArrayWriterPorts]:
        return self._output_runtime.array_out_ports

    # inPorts @0 () -> (ports :List(Component.Port));
    @override
    async def inPorts(self, _context, **kwargs):
        return self._port_runtime.in_port_messages()

    # connectInPort @1 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    @override
    async def connectInPort(self, name: str, sturdyRef, _context, **kwargs):
        return await self._port_runtime.connect_in_port(name, sturdyRef)

    # outPorts @2 () -> (ports :List(Component.Port));
    @override
    async def outPorts(self, _context, **kwargs):
        return self._port_runtime.out_port_messages()

    # connectOutPort @3 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    @override
    async def connectOutPort(self, name: str, sturdyRef, _context, **kwargs):
        return await self._port_runtime.connect_out_port(name, sturdyRef)

    # configEntries @4 () -> (config :List(ConfigEntry));
    @override
    async def configEntries(self, _context, **kwargs):
        return [
            fbp_capnp.Process.ConfigEntry.new_message(
                name=item[0],
                val=config_value_from_python(item[1]),
            )
            for item in self.raw_config.items()
        ]

    @override
    async def setConfigEntry(self, name, val, _context, **kwargs):
        self.apply_config_values({name: python_value_from_capnp_value(val)})

    async def transition_to_state(self, new_state: ProcessStateEnum):
        await self._state_runtime.transition_to_state(new_state)

    async def transition_to_activity(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
        delay_processing: bool = True,
    ) -> None:
        await self._state_runtime.transition_to_activity(new_state, port, delay_processing)

    # start @5 ();
    @override
    async def start(self, _context, **kwargs) -> bool:
        return await self._lifecycle_runtime.start()

    # stop @6 () -> (stopped :Bool);
    @override
    async def stop(self, _context, **kwargs) -> bool:
        return await self._lifecycle_runtime.stop()

    async def run(self):
        logger.warning("run method unimplemented")

    # state @8 (transitionCallback :StateTransition) -> (currentState :State);
    @override
    async def state(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.context.status.state_transition_callbacks.append(transitionCallback)
        return self.context.status.process_state

    # activity @10 (transitionCallback :ActivityTransition) -> (currentActivity :ActivityInfo);
    @override
    async def activity(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.context.status.activity_transition_callbacks.append(transitionCallback)
        return self._state_runtime.activity_message()

    # lastRun @9 () -> (info :RunInfo);
    @override
    async def lastRun(self, _context, **kwargs):
        return self._last_run_message()

    @property
    def stopping(self) -> bool:
        return self.context.lifecycle.stop_requested.is_set()

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
            msg = f"{self.name} received invalid config on port '{name}': {e}"
            raise ProcessConfigError(msg, port=name) from e

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
        await self._lifecycle_runtime.close_in_ports()

    async def force_close_ports(self):
        await self._lifecycle_runtime.force_close_ports()

    async def close_out_ports(self, *, cancel_pending_writes: bool | None = None):
        await self._lifecycle_runtime.close_out_ports(cancel_pending_writes=cancel_pending_writes)

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
            await self._lifecycle_runtime.force_close_ports()
