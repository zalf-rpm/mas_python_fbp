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
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    skip_lines = config["skip_lines"]
    if config["file"] and ports["out"]:
        try:
            with open(config["file"]) as _:
                if config["lines_mode"]:
                    for line in _.readlines():
                        if skip_lines > 0:
                            skip_lines -= 1
                            continue

                        out_ip = fbp_capnp.IP.new_message()
                        if config["to_attr"] and len(config["to_attr"]) > 0:
                            out_ip.attributes = [
                                {"key": config["to_attr"], "value": line}
                            ]
                        else:
                            out_ip.content = line
                        await ports["out"].write(value=out_ip)
                else:
                    file_content = _.read()
                    out_ip = fbp_capnp.IP.new_message()
                    if config["to_attr"] and len(config["to_attr"]) > 0:
                        out_ip.attributes = [
                            {"key": config["to_attr"], "value": file_content}
                        ]
                    else:
                        out_ip.content = file_content
                    await ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: RPC Exception:",
                e.description,
            )

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": None,
    "file": None,
    "lines_mode": False,
    "skip_lines": 0,
    "opt:to_attr": "[string] -> store read file content into 'to_attr'",
    "opt:file": "[string] -> path to file to read",
    "opt:lines_mode": "[true | false] -> send single lines if true else send whole file content at once",
    "opt:skip_lines": "[int] -> if lines mode is true, skip that many lines at the beginning of the file",
    "port:conf": "[TOML string] -> component configuration",
    "port:out": "[string] -> lines or whole file read",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Read a text file")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
