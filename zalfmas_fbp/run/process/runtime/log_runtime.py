from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.run.logging_config import format_exception_full
from zalfmas_fbp.run.metadata import LOG_PORT_NAME

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import LogMessageBuilder
    from mas.schema.fbp.fbp_capnp.types.enums import LogMessageLevelEnum

    from zalfmas_fbp.run.process.identity import ProcessIdentityContext

    from .output_runtime import OutputRuntime

logger = logging.getLogger(__name__)

#: Records held while waiting to be written. Oldest are dropped when full, so a burst of logging
#: costs memory that is bounded rather than blocking the component that produced it.
DEFAULT_QUEUE_SIZE: Final[int] = 1024
MAX_MESSAGE_CHARS: Final[int] = 4096
MAX_TRACEBACK_LINES: Final[int] = 100

_LEVEL_NAMES: Final[dict[int, str]] = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "critical",
}


def level_name_for(levelno: int) -> LogMessageLevelEnum:
    """The LogMessage.Level covering a Python log level."""
    for threshold in sorted(_LEVEL_NAMES, reverse=True):
        if levelno >= threshold:
            return _LEVEL_NAMES[threshold]  # pyright: ignore[reportReturnType]
    return "debug"


class _QueueHandler(logging.Handler):
    """Puts records on a bounded queue. Never blocks, never writes, never raises."""

    def __init__(self, queue: deque[logging.LogRecord], on_drop: Callable[[], None]) -> None:
        super().__init__()
        self._queue: deque[logging.LogRecord] = queue
        self._on_drop: Callable[[], None] = on_drop

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if len(self._queue) == self._queue.maxlen:
                self._on_drop()
            self._queue.append(record)
        except Exception:  # noqa: BLE001 - a logging handler must never propagate
            self.handleError(record)


class LogPortTee:
    """Mirrors this process's log records onto the runtime-owned ``log`` port.

    A *tee*, not a replacement (plan section 6.2): the local logger keeps its own handlers, so
    records emitted before the port is connected or after it closes are still seen. The port gets
    its own level, usually more verbose than stderr.

    Writes go through ``write_out_if_space`` and are dropped when the channel is full, so a slow or
    stalled log consumer can never stall the flow it observes. Failures on the log path itself go to
    the local logger only, never back onto the port.
    """

    def __init__(
        self,
        *,
        identity: ProcessIdentityContext,
        output_runtime: OutputRuntime,
        stop_event: asyncio.Event,
        level: int | str = logging.INFO,
        port_name: str = LOG_PORT_NAME,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self._identity: ProcessIdentityContext = identity
        self._output_runtime: OutputRuntime = output_runtime
        self._stop_event: asyncio.Event = stop_event
        self._port_name: str = port_name
        self._queue: deque[logging.LogRecord] = deque(maxlen=queue_size)
        self._handler: _QueueHandler | None = None
        self._task: asyncio.Task[None] | None = None
        self._level: int = logging.getLevelNamesMapping()[level.upper()] if isinstance(level, str) else level
        self.dropped_full_queue: int = 0
        self.dropped_full_channel: int = 0
        self.written: int = 0

    @property
    def connected(self) -> bool:
        return self._output_runtime.out_ports.get(self._port_name) is not None

    def start(self) -> None:
        if self._task is not None or not self.connected:
            return
        self._handler = _QueueHandler(self._queue, self._note_queue_drop)
        self._handler.setLevel(self._level)
        logging.getLogger().addHandler(self._handler)
        self._task = asyncio.create_task(
            self._drain(),
            name=f"{self._identity.name or self._identity.id}-log-tee",
        )

    def _note_queue_drop(self) -> None:
        self.dropped_full_queue += 1

    def message_for(self, record: logging.LogRecord) -> LogMessageBuilder:
        message = record.getMessage()
        if len(message) > MAX_MESSAGE_CHARS:
            message = f"{message[:MAX_MESSAGE_CHARS]}... (+{len(message) - MAX_MESSAGE_CHARS} chars)"

        traceback: list[str] = []
        if record.exc_info and record.exc_info[1] is not None:
            traceback = [line.rstrip("\n") for line in format_exception_full(record.exc_info[1])]
            if len(traceback) > MAX_TRACEBACK_LINES:
                dropped = len(traceback) - MAX_TRACEBACK_LINES
                traceback = [*traceback[:MAX_TRACEBACK_LINES], f"... (+{dropped} more lines)"]

        return fbp_capnp.LogMessage.new_message(
            level=level_name_for(record.levelno),
            timestamp=datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            processId=self._identity.id or "",
            processName=self._identity.name or "",
            logger=record.name,
            message=message,
            traceback=traceback,
        )

    async def _drain(self) -> None:
        while not self._stop_event.is_set():
            if not self._queue:
                await asyncio.sleep(0.05)
                continue
            record = self._queue.popleft()
            try:
                ip = fbp_capnp.IP.new_message(content=self.message_for(record))
                if await self._output_runtime.write_out_if_space(self._port_name, ip):
                    self.written += 1
                else:
                    self.dropped_full_channel += 1
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the drain loop must survive anything a record throws
                # Deliberately not logged: reporting a log-path failure would feed the port that
                # just failed, which is how a log loop starts.
                self.dropped_full_channel += 1

    async def close(self) -> None:
        handler, self._handler = self._handler, None
        if handler is not None:
            logging.getLogger().removeHandler(handler)

        task, self._task = self._task, None
        if task is not None and not task.done():
            _ = task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                logger.debug("%s log tee task ended", self._identity.name, exc_info=True)

        if self.dropped_full_queue or self.dropped_full_channel:
            logger.warning(
                "%s dropped %d log record(s) (%d queue full, %d channel full) of %d",
                self._identity.name,
                self.dropped_full_queue + self.dropped_full_channel,
                self.dropped_full_queue,
                self.dropped_full_channel,
                self.written + self.dropped_full_queue + self.dropped_full_channel,
            )
