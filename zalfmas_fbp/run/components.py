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

import argparse
import asyncio
import json
import subprocess as sp
import sys

import capnp


def run_component_from_metadata(func, meta):
    parser = create_default_fbp_component_args_parser(meta["component"]["info"]["description"])
    port_infos_reader_sr, default_config, _ = handle_default_fpb_component_args(parser, meta)
    asyncio.run(capnp.run(func(port_infos_reader_sr, default_config)))


def create_default_fbp_component_args_parser(component_description):
    parser = argparse.ArgumentParser(description=component_description)
    parser.add_argument(
        "port_infos_reader_sr",
        type=str,
        nargs="?",
        help="Sturdy ref to reader capability for receiving sturdy refs to connected channels (via ports)",
    )
    parser.add_argument(
        "--output_json_default_config",
        "-o",
        action="store_true",
        help="Output JSON configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    parser.add_argument(
        "--output_json_component_metadata",
        "-O",
        action="store_true",
        help="Output JSON component metadata at commandline. To be used for configuring component service.",
    )
    parser.add_argument(
        "--write_json_default_config",
        "-w",
        type=str,
        help="Output JSON configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    parser.add_argument(
        "--write_json_component_metadata",
        "-W",
        type=str,
        help="Output JSON component metadata in the current directory. To be used for configuring component service.",
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    return parser


def handle_default_fpb_component_args(parser, component_meta: dict = None):
    args = parser.parse_args()
    if component_meta and (dc := component_meta.get("component", {}).get("defaultConfig", None)):
        default_config = {k: v.get("value", v) if v and type(v) is dict else v for k, v in dc.items()}
    else:
        default_config = {}

    if args.name is not None:
        default_config["name"] = args.name

    port_infos_reader_sr = None
    if args.output_json_default_config:
        print(json.dumps(default_config, indent=4))
        exit(0)
    elif args.write_json_default_config:
        with open(args.write_json_default_config, "w") as _:
            json.dump(default_config, _, indent=4)
            exit(0)
    elif args.output_json_component_metadata:
        print(json.dumps(component_meta, indent=4))
        exit(0)
    elif args.write_json_component_metadata:
        with open(args.write_json_component_metadata, "w") as _:
            json.dump(component_meta, _, indent=4)
            exit(0)
    elif args.port_infos_reader_sr is not None:
        port_infos_reader_sr = args.port_infos_reader_sr
    else:
        parser.error("argument port_infos_reader_sr: expected sturdy ref")

    return port_infos_reader_sr, default_config, args


def start_local_component(path_to_executable: str, port_infos_reader_sr: str, name: str | None = None):
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    proc = sp.Popen(
        pte_split + [port_infos_reader_sr] + ([f'--name="{name}"'] if name else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc
