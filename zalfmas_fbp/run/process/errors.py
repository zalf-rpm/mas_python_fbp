from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


class ProcessRuntimeError(RuntimeError):
    def __init__(self, message: str, *, phase: str, port: str | None = None):
        super().__init__(message)
        self.phase: str = phase
        self.port: str | None = port


class ProcessConfigError(ValueError):
    def __init__(self, message: str, *, port: str | None = None):
        super().__init__(message)
        self.phase: str = "config"
        self.port: str | None = port


class InputPortReadError(ProcessRuntimeError):
    def __init__(self, process_name: str | None, port: str, message: str):
        super().__init__(f"{process_name} failed reading input port '{port}': {message}", phase="read", port=port)


class OutputPortWriteError(ProcessRuntimeError):
    def __init__(self, process_name: str | None, port: str, message: str):
        super().__init__(f"{process_name} failed writing output port '{port}': {message}", phase="write", port=port)


@dataclass
class ProcessRunInfo:
    process_id: str | None
    process_name: str | None
    outcome: Literal["none", "completed", "stopped", "failed"]
    phase: str
    port: str | None
    detail_type: str
    message: str
    cause_type: str | None = None
    cause_message: str | None = None
    traceback: list[str] | None = None


ProcessErrorInfo = ProcessRunInfo
