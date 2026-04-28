from __future__ import annotations

import argparse
import logging

LOG_FORMAT = "%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL_CHOICES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


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
    logging.basicConfig(format=LOG_FORMAT, datefmt=DATE_FORMAT, level=default_level)
    logging.getLogger().setLevel(default_level)
