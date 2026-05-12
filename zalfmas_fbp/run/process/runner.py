from __future__ import annotations

import argparse
import asyncio
import json
import logging
import subprocess as sp
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import capnp

from zalfmas_fbp.run.argparse_utils import parse_args_typed
from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

if TYPE_CHECKING:
    from .process import Process

logger = logging.getLogger(__name__)


@dataclass
class ProcessArgs(argparse.Namespace):
    process_cap_writer_sr: str | None = None
    output_json_default_config: bool = False
    output_json_component_metadata: bool = False
    write_json_default_config: str | None = None
    write_json_component_metadata: str | None = None
    serve_bootstrap: bool = False
    host: str | None = None
    port: int | None = None
    name: str | None = None
    log_level: str = "WARNING"


def start_local_process_component(
    path_to_executable: str,
    process_cap_writer_sr: str,
    name: str | None = None,
    log_level: str | None = None,
) -> sp.Popen[str]:
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    return sp.Popen(
        pte_split
        + [process_cap_writer_sr]
        + ([f'--name="{name}"'] if name else [])
        + ([f"--log_level={log_level}"] if log_level else []),
        text=True,
    )


def create_default_args_parser(component_description: str):
    parser = argparse.ArgumentParser(description=component_description)
    _ = parser.add_argument(
        "process_cap_writer_sr",
        type=str,
        nargs="?",
        help="SturdyRef to the Writer[fbp.capnp:Process]. Writes process capability on startup to writer.",
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
        "-b",
        "--serve_bootstrap",
        action="store_true",
        help="Serve process as the bootstrap object.>",
    )
    _ = parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    add_log_level_argument(parser)
    return parser


def run_process_from_metadata_and_cmd_args(
    p: Process[Any],
    component_meta: ComponentMetadata,
):
    parser = create_default_args_parser(component_description=p.description)
    args = parse_args_typed(parser, ProcessArgs)
    configure_logging(args.log_level)
    metadata = component_meta
    default_config = metadata.default_config_values()
    metadata_json = metadata.model_dump(mode="json", exclude_none=True)
    if args.name is not None:
        p.name = args.name
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
    if args.process_cap_writer_sr:
        asyncio.run(
            capnp.run(
                p.serve(
                    writer_sr=args.process_cap_writer_sr,
                    serve_bootstrap=args.serve_bootstrap,
                    host=args.host,
                    port=args.port,
                ),
            ),
        )
    else:
        logger.error("A sturdy ref to a writer capability is necessary to start the process.")
