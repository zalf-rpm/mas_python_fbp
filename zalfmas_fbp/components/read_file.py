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
import capnp
import os
import sys
import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.components as c
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr,
                                                              ins=["conf", "attr"], outs=["out"])
    await p.update_config_from_port(config, ports["conf"])

    skip_lines = int(config["skip_lines"])
    if config["file"]:
        try:
            attr = None
            if ports["attr"]:
                attr_msg = await ports["attr"].read()
                if attr_msg.which() != "done":
                    attr_ip = attr_msg.value.as_struct(fbp_capnp.IP)
                    attr = attr_ip.content

            if ports["out"]:
                with open(config["file"]) as _:
                    if config["lines_mode"]:
                        for line in _.readlines():
                            if skip_lines > 0:
                                skip_lines -= 1
                                continue

                            out_ip = fbp_capnp.IP.new_message(content=line)
                            if attr and config["to_attr"]:
                                out_ip.attributes = [{"key": config["to_attr"], "value": attr}]
                            await ports["out"].write(value=out_ip)
                    else:
                        file_content = _.read()
                        out_ip = fbp_capnp.IP.new_message(content=file_content)
                        if attr and config["to_attr"]:
                            out_ip.attributes = [{"key": config["to_attr"], "value": attr}]
                        await ports["out"].write(value=out_ip)
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

default_config = {
    "to_attr": "setup",
    "file": "/home/berg/Desktop/bahareh/run_cmd.txt",
    "lines_mode": True,
    "skip_lines": 0,

    "opt:to_attr": "[string] -> store received content from connected 'attr' port under this name in attributes section of IP",
    "opt:file": "[string] -> path to file to read",
    "opt:lines_mode": "[true | false] -> send single lines if true else send whole file content at once",
    "opt:skip_lines": "[int] -> if lines mode is true, skip that many lines at the beginning of the file",

    "port:conf": "[TOML string] -> component configuration",
    "port:attr": "[anypointer] -> arbitrary content to store as attached attribute with name 'to_attr'",
    "port:out": "[string] -> lines or whole file read",
}
def main():
    parser = c.create_default_fbp_component_args_parser("Read a text file")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(parser, default_config)
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))

if __name__ == '__main__':
    main()
