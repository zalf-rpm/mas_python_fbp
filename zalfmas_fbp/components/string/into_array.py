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
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    no_of_elements = 0 if "no_of_elements" == "all" else int(config["no_of_elements"])

    cast_to = None
    if config["cast_to"] == "float":
        cast_to = lambda v: float(v)
    elif config["cast_to"] == "int":
        cast_to = lambda v: int(v)

    init_list = lambda any_p, len_: any_p.init_as_list(
        capnp._ListSchema(capnp.types.Text), len_
    )
    if config["cast_to"] == "float":
        init_list = lambda any_p, len_: any_p.init_as_list(
            capnp._ListSchema(capnp.types.Float64), len_
        )
    elif config["cast_to"] == "int":
        init_list = lambda any_p, len_: any_p.init_as_list(
            capnp._ListSchema(capnp.types.Int64), len_
        )

    while ports["in"] and ports["out"]:
        try:
            elems = []
            while no_of_elements == 0 or len(elems) < no_of_elements:
                in_msg = await ports["in"].read()
                if in_msg.which() == "done":
                    ports["in"] = None
                    break

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                elems.append(s)

            if cast_to:
                elems = list(map(cast_to, elems))
            print(f"{os.path.basename(__file__)}:", elems)

            req = ports["out"].write_request()
            l = init_list(req.value.as_struct(fbp_capnp.IP).content, len(elems))
            for i, val in enumerate(elems):
                l[i] = val
            await req.send()

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "no_of_elements": "all",
    "cast_to": "text",
    "opt:cast_to": "[text | float | int] -> cast text elements to these types",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "string -> string to add to array",
    "port:out": "[list[text | float | int]] -> output list cast to cast_to type",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Split a string.")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
