from __future__ import annotations

from dataclasses import dataclass


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
