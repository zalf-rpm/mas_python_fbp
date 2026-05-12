from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from zalfmas_fbp.run.metadata import ComponentMetadata

from .errors import ProcessRunInfo
from .types import ArrayOutWriteTasks, ArrayReaderPorts, ArrayWriterPorts, ProcessConfig, RawConfig

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import (
        ActivityTransitionClient,
        ReaderClient,
        StateTransitionClient,
        WriterClient,
    )
    from mas.schema.fbp.fbp_capnp.types.enums import (
        ProcessActivityStateEnum,
        ProcessStateEnum,
    )


@dataclass
class ProcessConfigState:
    raw_config: RawConfig = field(default_factory=dict)
    config: ProcessConfig | RawConfig = field(default_factory=dict)


@dataclass
class ProcessPortState:
    in_ports: dict[str, ReaderClient | None] = field(default_factory=dict)
    array_in_ports: dict[str, ArrayReaderPorts] = field(default_factory=dict)
    array_in_buffers: dict[str, dict[int, object]] = field(default_factory=dict)
    out_ports: dict[str, WriterClient | None] = field(default_factory=dict)
    array_out_ports: dict[str, ArrayWriterPorts] = field(default_factory=dict)
    array_out_next_indices: dict[str, int] = field(default_factory=dict)
    array_out_write_tasks: dict[str, ArrayOutWriteTasks] = field(default_factory=dict)


@dataclass
class ProcessLifecycleState:
    run_task: asyncio.Task[None] | None = None
    run_exception: BaseException | None = None
    last_run: ProcessRunInfo | None = None
    stop_requested: asyncio.Event = field(default_factory=asyncio.Event)
    soft_stop_timeout_seconds: float = 30.0


@dataclass
class ProcessStatusState:
    process_state: ProcessStateEnum = "idle"
    state_transition_callbacks: list[StateTransitionClient] = field(default_factory=list)
    activity_state: ProcessActivityStateEnum = "none"
    activity_port: str = ""
    activity_transition_callbacks: list[ActivityTransitionClient] = field(default_factory=list)
    pending_processing_activity_task: asyncio.Task[None] | None = None


@dataclass
class ProcessContext:
    metadata: ComponentMetadata | None = None
    config: ProcessConfigState = field(default_factory=ProcessConfigState)
    ports: ProcessPortState = field(default_factory=ProcessPortState)
    lifecycle: ProcessLifecycleState = field(default_factory=ProcessLifecycleState)
    status: ProcessStatusState = field(default_factory=ProcessStatusState)
