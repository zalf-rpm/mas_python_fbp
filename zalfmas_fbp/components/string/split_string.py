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
                                                              ins=["conf", "in"], outs=["out"])
    print(f"{os.path.basename(__file__)}: {config['name']} connected port(s)")
    await p.update_config_from_port(config, ports["conf"])
    if ports["conf"]:
        print(f"{os.path.basename(__file__)}: {config['name']} updated config from config port")

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            s : str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            print(f"{os.path.basename(__file__)}: {config['name']} received:", s)
            s = s.rstrip()
            vals = s.split(config["split_at"])

            for val in vals:
                out_ip = fbp_capnp.IP.new_message(content=val)
                await ports["out"].write(value=out_ip)
                print(f"{os.path.basename(__file__)}: {config['name']} sent:", val)

        except Exception as e:
            print(f"{os.path.basename(__file__)}: {config['name']} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: {config['name']} process finished")

default_config = {
    "name": "split_string",
    "split_at": ",",

    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string] -> string to split",
    "port:out": "[list[text | float | int]] -> output split list cast to cast_to type"
}
def main():
    parser = c.create_default_fbp_component_args_parser("Split a string.")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(parser, default_config)
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))

if __name__ == '__main__':
    main()
