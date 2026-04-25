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
from zalfmas_common import common, geo

from zalfmas_fbp.run import components as c
from zalfmas_fbp.run import ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "geo", "name": "Geo"},
    "component": {
        "info": {
            "id": "b753df51-40f1-4778-ac47-82858c8ef80c",
            "name": "Proj transform coords",
            "description": "Transform coordinates using the Proj library.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "in",
                "contentType": "geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
                "desc": "Input geo coordinate.",
            },
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
                "desc": "Output geo coordinate.",
            }
        ],
        "defaultConfig": {
            "from_name": {
                "value": "LatLon",
                "type": "string",
                "desc": "Source CRS name: One of LatLon, WGS84, GKx (x=2-5), UTMab (a=[1-60], b=[C-X]).",
            },
            "to_name": {
                "value": "LatLon",
                "type": "string",
                "desc": "Target CRS name: One of LatLon, WGS84, GKx (x=2-5), UTMab (a=[1-60], b=[C-X]).",
            },
            "from_attr": {"value": None, "type": "string", "desc": "Attribute name to use as the input coordinate."},
            "to_attr": {"value": None, "type": "string", "desc": "Attribute name to use as the output."},
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    from_type = geo.name_to_struct_type(config["from_name"])
    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
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
            await pc.out_ports["out"].write(value=out_ip)

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
