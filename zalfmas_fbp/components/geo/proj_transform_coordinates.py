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

from zalfmas_fbp.run import components as c
from zalfmas_fbp.run import ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    from_type = geo.name_to_struct_type(config["from_name"])
    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            if attr:
                from_coord = attr.as_struct(from_type)
            else:
                from_coord = in_ip.content.as_struct(from_type)
            to_coord = geo.transform_from_to_geo_coord(from_coord, config["to_name"])
            out_ip = fbp_capnp.IP.new_message()
            if not config["to_attr"]:
                out_ip.content = to_coord
            common.copy_and_set_fbp_attrs(
                in_ip,
                out_ip,
                **({config["to_attr"]: to_coord} if config["to_attr"] else {}),
            )
            await ports["out"].write(value=out_ip)

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
    "from_name": "utm32n",
    "to_name": "latlon",
    "from_attr": None,
    "to_attr": None,
    "opt:from_name": "[string (known name like utm32n)] -> source CRS -> refer to know name in zalfmas_common.geo lib",
    "opt:to_name": "[string (known name like latlon)] -> target CRS -> refer to know name in zalfmas_common.geo lib",
    "opt:from_attr": "[name:string] -> get sturdy ref or capability from attibute 'from_attr'",
    "opt:to_attr": "[name:string] -> send data attached to attribute 'to_attr'",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[geo_capnp:LatLonCoord | geo_capnp:UTMCoord | geo_capnp:GKCoord] -> input geo coord",
    "port:out": "[geo_capnp:LatLonCoord | geo_capnp:UTMCoord | geo_capnp:GKCoord] -> transformed geo coord",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Get (all) timeseries from dataset at 'ds' input port"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
