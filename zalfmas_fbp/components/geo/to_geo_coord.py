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

import logging
import os

import capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import geo

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "geo", "name": "Geo"},
    "component": {
        "info": {
            "id": "66ea3fce-80f7-4ab6-b77a-0966cb7c2793",
            "name": "to geo coord",
            "description": "Create [geo_capnp:LatLonCoord | geo_capnp:UTMCoord | geo_capnp:GKCoord] from a list of values.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "vals",
                "contentType": "List[float | int]",
                "desc": "Values to convert into coord.",
            },
        ],
        "outPorts": [
            {
                "name": "coord",
                "contentType": "geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
                "desc": "Coord to output.",
            },
        ],
        "defaultConfig": {
            "to_name": {
                "value": "LatLon",
                "type": "string",
                "desc": "One of 2D, XY, LatLon, WGS84, GKx (x=2-5), UTMab (a=[1-60], b=[C-X])",
            },
            "list_type": {"value": "float", "type": ["float", "int"]},
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "vals"], outs=["coord"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    to_instance = geo.name_to_struct_instance(config["to_name"])
    if config["list_type"] == "float":
        list_schema_type = capnp._ListSchema(capnp.types.Float64)
    else:
        list_schema_type = capnp._ListSchema(capnp.types.Int64)

    while pc.in_ports["vals"] and pc.out_ports["coord"]:
        try:
            in_msg = await pc.in_ports["vals"].read()
            if in_msg.which() == "done":
                pc.in_ports["vals"] = None
                continue

            vals = in_msg.value.as_struct(fbp_capnp.IP).content.as_list(list_schema_type)
            if len(vals) > 1:
                to_coord = to_instance.copy()
                geo.set_xy(to_coord, vals[0], vals[1])
                await pc.out_ports["coord"].write(value=fbp_capnp.IP.new_message(content=to_coord))
            else:
                raise Exception("Not enough values in list. Need at least two for a coordinate.")

        except capnp.KjException as e:
            logger.error("%s: %s RPC Exception: %s", os.path.basename(__file__), config["name"], e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
