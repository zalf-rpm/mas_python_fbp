from __future__ import annotations

import argparse
import logging
from typing import Any

from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging


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
