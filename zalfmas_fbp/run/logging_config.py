from __future__ import annotations

import argparse
import linecache
import logging
import traceback

LOG_FORMAT = "%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL_CHOICES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _format_frame_source_lines(frame: traceback.FrameSummary) -> list[str]:
    end_lineno = frame.end_lineno or frame.lineno
    if end_lineno < frame.lineno:
        end_lineno = frame.lineno

    formatted_lines: list[str] = []
    for lineno in range(frame.lineno, end_lineno + 1):
        source_line = linecache.getline(frame.filename, lineno)
        if source_line:
            formatted_lines.append(f"    {source_line}")

    if not formatted_lines and frame.line:
        formatted_lines.append(f"    {frame.line}\n")

    return formatted_lines


def _format_traceback_exception(traceback_exception: traceback.TracebackException) -> list[str]:
    lines: list[str] = []

    if traceback_exception.__cause__ is not None:
        lines.extend(_format_traceback_exception(traceback_exception.__cause__))
        lines.append("\nThe above exception was the direct cause of the following exception:\n\n")
    elif traceback_exception.__context__ is not None and not traceback_exception.__suppress_context__:
        lines.extend(_format_traceback_exception(traceback_exception.__context__))
        lines.append("\nDuring handling of the above exception, another exception occurred:\n\n")

    lines.append("Traceback (most recent call last):\n")
    for frame in traceback_exception.stack:
        lines.append(f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}\n')
        lines.extend(_format_frame_source_lines(frame))
    lines.extend(traceback_exception.format_exception_only())
    return lines


def format_exception_full(exc: BaseException) -> list[str]:
    return _format_traceback_exception(
        traceback.TracebackException.from_exception(exc, capture_locals=False),
    )


class FullTracebackFormatter(logging.Formatter):
    def formatException(self, ei) -> str:  # noqa: N802
        exc_type, exc_value, exc_traceback = ei
        if exc_value is None:
            return super().formatException(ei)
        return "".join(
            _format_traceback_exception(
                traceback.TracebackException(
                    exc_type,
                    exc_value,
                    exc_traceback,
                    capture_locals=False,
                ),
            ),
        )


def add_log_level_argument(parser: argparse.ArgumentParser, *, default_level: str = "WARNING") -> None:
    _ = parser.add_argument(
        "-l",
        "--log_level",
        type=str,
        choices=LOG_LEVEL_CHOICES,
        default=default_level,
        help="Set logging level.",
    )


def configure_logging(default_level: str = "WARNING") -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.addHandler(logging.StreamHandler())

    formatter = FullTracebackFormatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)

    root_logger.setLevel(default_level)
