from __future__ import annotations

import argparse
from collections.abc import Sequence


def parse_args_typed[T: argparse.Namespace](
    parser: argparse.ArgumentParser,
    namespace_type: type[T],
    argv: Sequence[str] | None = None,
) -> T:
    namespace = namespace_type()
    parser.parse_args(argv, namespace=namespace)
    return namespace
