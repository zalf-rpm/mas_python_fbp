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

import asyncio
import argparse
import capnp
import os
from pathlib import Path
import subprocess as sp
import sys
import tomlkit as tk
import uuid
from zalfmas_common import common
from zalfmas_common import service as serv
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp


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
    return parser

def handle_default_fpb_component_args(parser, config: dict=None):
    args = parser.parse_args()

    doc = tk.document()
    doc.add(tk.comment(f"{parser.prog} FBP component configuration (data and documentation)"))
    doc.add(tk.comment("The 'defaults' section shows the configuration settings in TOML format which can be send to the config port of the component."))
    defaults = tk.table()
    ports = tk.table()
    options = tk.table()
    remove_keys = []
    if config:
        for k, v in config.items():
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

    port_infos_reader_sr = None
    if args.output_toml_config:
        print(tk.dumps(doc))
        exit(0)
    elif args.write_toml_config:
        with open(args.write_toml_config, "w") as _:
            tk.dump(doc, _)
            exit(0)
    elif args.port_infos_reader_sr is not None:
        port_infos_reader_sr = args.port_infos_reader_sr
    else:
        parser.error("argument port_infos_reader_sr: expected sturdy ref")

    for k in remove_keys:
        del config[k]
    return port_infos_reader_sr, config, args


def start_component(path_to_executable, port_infos_reader_sr):
    proc = sp.Popen(list(path_to_executable.split(" ")) + [port_infos_reader_sr],
                    #stdout=sp.PIPE, stderr=sp.STDOUT,
                    text=True)
    return proc

class Component(fbp_capnp.Component.Server, common.Identifiable):

    def __init__(self, path_to_executable, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc = None

    async def start(self, portInfosReaderSr, _context):  # start @0 (portInfosReaderSr :Text) -> (success :Bool);
        self.proc = start_component(self.path_to_executable, portInfosReaderSr)
        return self.proc.poll() is None

    async def stop(self, _context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            return rt
        return False

async def main(path_to_executable=None, serve_bootstrap=True, host=None, port=None,
               id=None, name="FBP component service", description=None, srt=None):
    config = {
        "path_to_executable": path_to_executable,
        "port": port,
        "host": host,
        "id": id,
        "name": name,
        "description": description,
        "serve_bootstrap": serve_bootstrap,
        "srt": srt
    }
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    restorer = common.Restorer()
    service = Component(config["path_to_executable"], id, name, description)
    name_to_service = {"service": service}
    await serv.init_and_run_service(name_to_service, config["host"], config["port"],
                                    serve_bootstrap=config["serve_bootstrap"], restorer=restorer,
                                    name_to_service_srs={"service": config["srt"]})

if __name__ == '__main__':
    asyncio.run(capnp.run(main(serve_bootstrap=True)))