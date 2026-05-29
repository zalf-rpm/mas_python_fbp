from __future__ import annotations

import argparse
from dataclasses import dataclass

from zalfmas_fbp.run.argparse_utils import parse_args_typed


@dataclass
class _Args(argparse.Namespace):
    name: str | None = None
    count: int = 0
    verbose: bool = False


def test_parse_args_typed_returns_namespace_subclass() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--name", type=str)
    _ = parser.add_argument("--count", type=int, default=0)
    _ = parser.add_argument("--verbose", action="store_true")

    args = parse_args_typed(parser, _Args, ["--name", "demo", "--count", "3", "--verbose"])

    assert isinstance(args, _Args)
    assert args.name == "demo"
    assert args.count == 3
    assert args.verbose is True


def test_parse_args_typed_keeps_namespace_defaults() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--name", type=str)

    args = parse_args_typed(parser, _Args, [])

    assert isinstance(args, _Args)
    assert args.name is None
    assert args.count == 0
    assert args.verbose is False
