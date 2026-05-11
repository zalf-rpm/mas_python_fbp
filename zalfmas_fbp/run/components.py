#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess as sp
import sys
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import capnp

from zalfmas_fbp.run.argparse_utils import parse_args_typed
from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata


@dataclass
class ComponentArgs(argparse.Namespace):
    port_infos_reader_sr: str | None = None
    output_json_default_config: bool = False
    output_json_component_metadata: bool = False
    write_json_default_config: str | None = None
    write_json_component_metadata: str | None = None
    name: str | None = None
    log_level: str = "WARNING"


def run_component_from_metadata(
    func: Callable[[str, dict[str, Any]], Coroutine[Any, Any, Any]],
    metadata: ComponentMetadata,
):
    parser = create_default_fbp_component_args_parser(metadata.info.description)
    port_infos_reader_sr, default_config, args = handle_default_fpb_component_args(parser, metadata)
    runtime_config = default_config.copy()
    runtime_config["name"] = args.name or metadata.info.name
    asyncio.run(capnp.run(func(port_infos_reader_sr, runtime_config)))


def create_default_fbp_component_args_parser(component_description: str | None):
    parser = argparse.ArgumentParser(description=component_description)
    _ = parser.add_argument(
        "port_infos_reader_sr",
        type=str,
        nargs="?",
        help="Sturdy ref to reader capability for receiving sturdy refs to connected channels (via ports)",
    )
    _ = parser.add_argument(
        "--output_json_default_config",
        "-o",
        action="store_true",
        help="Output JSON configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--output_json_component_metadata",
        "-O",
        action="store_true",
        help="Output JSON component metadata at commandline. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "--write_json_default_config",
        "-w",
        type=str,
        help="Output JSON configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--write_json_component_metadata",
        "-W",
        type=str,
        help="Output JSON component metadata in the current directory. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    add_log_level_argument(parser)
    return parser


def handle_default_fpb_component_args(parser: argparse.ArgumentParser, component_meta: ComponentMetadata | None = None):
    args = parse_args_typed(parser, ComponentArgs)
    configure_logging(args.log_level)
    default_config = component_meta.default_config_values() if component_meta is not None else {}
    metadata_json = component_meta.model_dump(mode="json", exclude_none=True) if component_meta is not None else {}

    port_infos_reader_sr = None
    if args.output_json_default_config:
        _ = sys.stdout.write(json.dumps(default_config, indent=4) + "\n")
        exit(0)
    elif args.write_json_default_config:
        with Path(args.write_json_default_config).open("w") as _:
            json.dump(default_config, _, indent=4)
            exit(0)
    elif args.output_json_component_metadata:
        _ = sys.stdout.write(json.dumps(metadata_json, indent=4) + "\n")
        exit(0)
    elif args.write_json_component_metadata:
        with Path(args.write_json_component_metadata).open("w") as _:
            json.dump(metadata_json, _, indent=4)
            exit(0)
    elif args.port_infos_reader_sr is not None:
        port_infos_reader_sr = args.port_infos_reader_sr
    else:
        parser.error("argument port_infos_reader_sr: expected sturdy ref")

    return port_infos_reader_sr, default_config, args


def start_local_component(
    path_to_executable: str,
    port_infos_reader_sr: str,
    name: str | None = None,
    log_level: str | None = None,
):
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    proc = sp.Popen(
        pte_split
        + [port_infos_reader_sr]
        + ([f'--name="{name}"'] if name else [])
        + ([f"--log_level={log_level}"] if log_level else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc
