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
from zalfmas_common import common, geo
from zalfmas_common import rect_ascii_grid_management as ragm

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    debug_out = config["debug_out"]
    if not config["path_to_grid"]:
        raise Exception("No path_to_grid given at start of component.")

    grid_data = ragm.load_grid_cached(
        config["path_to_grid"],
        int if config["type"] == "int" else float,
        print_path=debug_out,
    )

    while ports["in"] and ports["out"]:
        try:
            msg = await ports["in"].read()
            # check for end of data from in port
            if msg.which() == "done":
                ports["in"] = None
                break

            in_ip = msg.value.as_struct(fbp_capnp.IP)
            ll = in_ip.content.as_struct(geo.name_to_struct_type("latlon"))
            val = grid_data["value"](ll.lat, ll.lon)
            out_ip = fbp_capnp.IP.new_message(content=str(val))
            common.copy_and_set_fbp_attrs(in_ip, out_ip)
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "path_to_grid": None,
    "type": "int",  # int | float
    "debug_out": True,  # true | false
    "split_at": ",",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[geo_capnp:LatLon]",  # coordinate
    "port:out": "[str]",  # value
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Get data a supplied coordinate from grid"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
