from __future__ import annotations

import argparse
import io
import logging
from typing import Any

from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging, format_exception_full


def _always_fail(*_args: str) -> bool:
    raise RuntimeError("boom")


def _raise_multiline_runtime_error() -> None:
    if not _always_fail(
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
    ):
        return


def test_add_log_level_argument_uses_default_level() -> None:
    parser = argparse.ArgumentParser()
    add_log_level_argument(parser, default_level="INFO")

    args = parser.parse_args([])

    assert args.log_level == "INFO"


def test_add_log_level_argument_accepts_explicit_level() -> None:
    parser = argparse.ArgumentParser()
    add_log_level_argument(parser)

    args = parser.parse_args(["--log_level", "ERROR"])

    assert args.log_level == "ERROR"


def test_configure_logging_uses_explicit_level_even_with_env_vars_set(monkeypatch: Any) -> None:
    original_level = logging.getLogger().level
    monkeypatch.setenv("ZALFMAS_FBP_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("LOG_LEVEL", "CRITICAL")

    try:
        configure_logging(default_level="INFO")
        assert logging.getLogger().level == logging.INFO
    finally:
        logging.getLogger().setLevel(original_level)


def test_format_exception_full_expands_multiline_frames() -> None:
    try:
        _raise_multiline_runtime_error()
    except RuntimeError as exc:
        formatted = "".join(format_exception_full(exc))
    else:
        msg = "multiline runtime error should have been raised"
        raise AssertionError(msg)

    assert "if not _always_fail(" in formatted
    assert '"a",' in formatted
    assert '"h",' in formatted
    assert "...<" not in formatted


def test_configure_logging_updates_existing_handlers_with_full_traceback_formatter() -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root_logger.handlers = [handler]

    try:
        configure_logging(default_level="INFO")
        logger = logging.getLogger("test.logging")

        try:
            _raise_multiline_runtime_error()
        except RuntimeError:
            logger.exception("captured")

        output = stream.getvalue()
        assert "captured" in output
        assert "if not _always_fail(" in output
        assert '"a",' in output
        assert '"h",' in output
        assert "...<" not in output
    finally:
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)


# --- frames without line numbers ---------------------------------------------------------------


def test_a_frame_without_a_line_number_does_not_crash_the_formatter() -> None:
    """`FrameSummary.lineno` is `int | None`. Comparing and adding it unconditionally raised
    TypeError inside the traceback formatter, so the crash replaced the traceback being read."""

    import traceback

    from zalfmas_fbp.run.logging_config import _format_frame_source_lines

    frame = traceback.FrameSummary("somewhere.py", None, "fn", line="the source line")
    assert _format_frame_source_lines(frame) == ["    the source line\n"]


def test_a_frame_without_a_line_number_or_source_yields_nothing() -> None:
    import traceback

    from zalfmas_fbp.run.logging_config import _format_frame_source_lines

    assert _format_frame_source_lines(traceback.FrameSummary("somewhere.py", None, "fn")) == []


def test_an_end_line_before_the_start_is_treated_as_a_single_line() -> None:
    import traceback

    from zalfmas_fbp.run.logging_config import _format_frame_source_lines

    frame = traceback.FrameSummary(__file__, 1, "fn")
    frame.end_lineno = 0
    assert len(_format_frame_source_lines(frame)) <= 1


def test_format_exception_full_survives_a_stack_with_an_unknown_line_number() -> None:
    """The whole formatter, not just the helper: this is the path a failing component takes."""

    import traceback

    from zalfmas_fbp.run.logging_config import _format_traceback_exception

    try:
        msg = "boom"
        raise ValueError(msg)
    except ValueError as exc:
        te = traceback.TracebackException.from_exception(exc, capture_locals=False)

    te.stack.append(traceback.FrameSummary("unknown.py", None, "mystery"))
    rendered = "".join(_format_traceback_exception(te))
    assert "ValueError: boom" in rendered
    assert "mystery" in rendered


def test_the_formatter_falls_back_when_there_is_no_exception() -> None:
    from zalfmas_fbp.run.logging_config import FullTracebackFormatter

    assert FullTracebackFormatter().formatException((None, None, None))
