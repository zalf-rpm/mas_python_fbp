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

import os

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common, geo
from zalfmas_common import rect_ascii_grid_management as ragm

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "geo", "name": "Geo"},
    "component": {
        "info": {
            "id": "d8e349d6-e0e0-49cb-a24f-0b42358791a5",
            "name": "get lat/lon grid value",
            "description": "Get value from a lat/lon grid.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {"name": "in", "contentType": "geo.capnp:LatLon", "desc": "Lat/Lon coordinate to get the value at."},
        ],
        "outPorts": [
            {"name": "out", "contentType": "common.capnp:Value", "desc": "Value at the given lat/lon coordinate."}
        ],
        "defaultConfig": {
            "path_to_grid": {"value": None, "type": "string", "desc": "Path to the lat/lon grid file."},
            "type": {"value": "int", "type": ["int", "float"], "desc": "Type of value to read from grid."},
            "debug_out": {
                "value": True,
                "type": "bool",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    debug_out = config["debug_out"]
    if not config["path_to_grid"]:
        raise Exception("No path_to_grid given at start of component.")

    grid_data = ragm.load_grid_cached(
        config["path_to_grid"],
        int if config["type"] == "int" else float,
        print_path=debug_out,
    )

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            msg = await pc.in_ports["in"].read()
            # check for end of data from in port
            if msg.which() == "done":
                pc.in_ports["in"] = None
                break

            in_ip = msg.value.as_struct(fbp_capnp.IP)
            ll = in_ip.content.as_struct(geo.name_to_struct_type("latlon"))
            val = grid_data["value"](ll.lat, ll.lon)
            if config["type"] == "int":
                cval = common_capnp.Value.new_message(i64=val)
            else:
                cval = common_capnp.Value.new_message(f64=val)
            out_ip = fbp_capnp.IP.new_message(content=cval)
            common.copy_and_set_fbp_attrs(in_ip, out_ip)
            await pc.out_ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
