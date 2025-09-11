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
from zalfmas_common import geo

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "vals"], outs=["coord"]
    )
    await p.update_config_from_port(config, ports["conf"])

    to_instance = geo.name_to_struct_instance(config["to_name"])
    if config["list_type"] == "float":
        list_schema_type = capnp._ListSchema(capnp.types.Float64)
    else:
        list_schema_type = capnp._ListSchema(capnp.types.Int64)

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            vals = in_msg.value.as_struct(fbp_capnp.IP).content.as_list(
                list_schema_type
            )
            if len(vals) > 1:
                to_coord = to_instance.copy()
                geo.set_xy(to_coord, vals[0], vals[1])
                await ports["out"].write(
                    value=fbp_capnp.IP.new_message(content=to_coord)
                )
            else:
                raise Exception(
                    "Not enough values in list. Need at least two for a coordinate."
                )

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
    "to_name": "wgs84",
    "list_type": "float",
    "opt:to_name": "wgs84",
    "opt:list_type": "float",  # float | int
    "port:conf": "[TOML string] -> component configuration",
    "port:vals": "[list[float | int] -> values to convert into coord",
    "port:coord": "[geo_capnp:LatLonCoord | geo_capnp:UTMCoord | geo_capnp:GKCoord] -> coord to output",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Read array (pair) of values and convert into geo coord"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
