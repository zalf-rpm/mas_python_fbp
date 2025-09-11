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
import subprocess as sp

import tomlkit as tk


def create_default_fbp_component_args_parser(component_description):
    parser = argparse.ArgumentParser(description=component_description)
    parser.add_argument(
        "port_infos_reader_sr",
        type=str,
        nargs="?",
        help="Sturdy ref to reader capability for receiving sturdy refs to connected channels (via ports)",
    )
    parser.add_argument(
        "--output_toml_config",
        "-o",
        action="store_true",
        help="Output TOML configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    parser.add_argument(
        "--write_toml_config",
        "-w",
        type=str,
        help="Create a TOML configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    return parser


def handle_default_fpb_component_args(parser, config: dict = None):
    args = parser.parse_args()
    if config is None:
        config = {}
    remove_keys = []

    def create_toml():
        doc = tk.document()
        doc.add(
            tk.comment(
                f"{parser.prog} FBP component configuration (data and documentation)"
            )
        )
        doc.add(
            tk.comment(
                "The 'defaults' section shows the configuration settings in TOML format which can be send to the config port of the component."
            )
        )
        defaults = tk.table()
        ports = tk.table()
        options = tk.table()
        if config:
            for k, v in config.items():
                if v is None:
                    continue
                if "port:" in k:
                    ports.add(k[5:], v)
                    remove_keys.append(k)
                elif "opt:" in k:
                    options.add(k[4:], v)
                    remove_keys.append(k)
                else:
                    defaults.add(k, v)
        if len(defaults) > 0:
            doc.add("defaults", defaults)
        if len(options) > 0:
            doc.add("options", options)
        if len(ports) > 0:
            doc.add("ports", ports)
        return doc

    if args.name is not None:
        config["name"] = args.name

    port_infos_reader_sr = None
    if args.output_toml_config:
        print(tk.dumps(create_toml()))
        exit(0)
    elif args.write_toml_config:
        with open(args.write_toml_config, "w") as _:
            tk.dump(create_toml(), _)
            exit(0)
    elif args.port_infos_reader_sr is not None:
        port_infos_reader_sr = args.port_infos_reader_sr
    else:
        parser.error("argument port_infos_reader_sr: expected sturdy ref")

    for k in remove_keys:
        del config[k]
    return port_infos_reader_sr, config, args


def start_local_component(path_to_executable, port_infos_reader_sr, name=None):
    proc = sp.Popen(
        list(path_to_executable.split(" "))
        + [port_infos_reader_sr]
        + ([f'--name="{name}"'] if name else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc
