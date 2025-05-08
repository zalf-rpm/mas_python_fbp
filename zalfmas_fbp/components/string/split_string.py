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
    await p.update_config_from_port(config, ports["conf"])

    cast_to = None
    if config["cast_to"] == "float":
        cast_to = lambda v: float(v)
    elif config["cast_to"] == "int":
        cast_to = lambda v: int(v)

    init_list = lambda any_p, len_: any_p.init_as_list(capnp._ListSchema(capnp.types.Text), len_)
    if config["cast_to"] == "float":
        init_list = lambda any_p, len_: any_p.init_as_list(capnp._ListSchema(capnp.types.Float64), len_)
    elif config["cast_to"] == "int":
        init_list = lambda any_p, len_: any_p.init_as_list(capnp._ListSchema(capnp.types.Int64), len_)

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            s : str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            s = s.rstrip()
            vals = s.split(config["split_at"])
            if cast_to:
                vals = list(map(cast_to, vals))
            #print("split_string vals:", vals)

            req = ports["out"].write_request()
            l = init_list(req.value.as_struct(fbp_capnp.IP).content, len(vals))
            for i, val in enumerate(vals):
                l[i] = val
            await req.send()
            #await ports["out"].write(value=vals)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

default_config = {
    "split_at": ",",
    "cast_to": "text",

    "opt:cast_to": "[text | float | int] -> cast text elements to these types",

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
