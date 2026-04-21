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

from pymep.realParser import eval as mep_eval
from zalfmas_capnp_schemas_with_stubs import (
    common_capnp,
    fbp_capnp,
    geo_capnp,
    grid_capnp,
)
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "grid", "name": "Grid"},
    "component": {
        "info": {
            "id": "cb6720d6-bc33-445d-b2c1-aa3842219c81",
            "name": "Use grid service",
            "description": "Use the grid service to get the grid value at a given Lat/Lon coord.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {"name": "in", "contentType": "geo.capnp:LatLonCoord", "desc": "The coordinate to get the value at."},
            {
                "name": "service",
                "contentType": "grid.capnp:Service | SturdyRef",
                "desc": "Capability or sturdy ref to service.",
            },
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "grid.capnp:Grid.Value | common.capnp:Value",
                "desc": "Output grid value at given coordinate.",
            }
        ],
        "defaultConfig": {
            "as_common_value": {
                "value": False,
                "type": "bool",
                "desc": "Send the output as a common.capnp:Value structure instead of grid.capnp:Grid.Value.",
            },
            "from_attr": {
                "value": None,
                "type": "string",
                "desc": "Attribute name to use as the input coordinate (a geo.capnp:LatLonCoord).",
            },
            "to_attr": {
                "value": None,
                "type": "string",
                "desc": "Attribute name to use as the output (a grid.capnp:Grid.Value or a common.capnp:Value).",
            },
            "calc": {
                "value": {
                    "f(gv)": None,
                },
                "type": "object",
                "desc": "If 'f(gv)' has a value, define an simple arithmetic expression named 'f(gv)', which can use 'gv' (grid value) and possible other variables defined in the 'calc' object.",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "in", "service"],
        outs=["out"],
    )
    await p.update_config_from_port(config, ports["conf"])

    service = None
    if ports["service"]:
        service = ports.read_or_connect("service", cast_as=grid_capnp.Service)
        if not service:
            print(f"{os.path.basename(__file__)} No soil service could be received or connected to.")
            return

    try:
        while ports["in"] and ports["out"] and service:
            in_msg = ports["in"].read().wait()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            if attr:
                coord = attr.as_struct(geo_capnp.LatLonCoord)
            else:
                coord = in_ip.content.as_struct(geo_capnp.LatLonCoord)

            grid_val = (await service.closestValueAt(coord)).val

            def maybe_as_common_value(grid_value):
                if config.get("as_common_value", False):
                    if grid_value.which() == "f":
                        return common_capnp.Value.new_message(f64=grid_value.f)
                    elif grid_value.which() == "i":
                        return common_capnp.Value.new_message(i64=grid_value.i)
                    elif grid_value.which() == "ui":
                        return common_capnp.Value.new_message(ui64=grid_value.ui)
                return grid_value

            def update_val(expr, var_name, grid_value):
                if grid_value.which() == "f":
                    grid_value.f = mep_eval(expr, {var_name, float(grid_value.f)})
                elif grid_value.which() == "i":
                    grid_value.i = int(mep_eval(expr, {var_name, int(grid_value.i)}))
                elif grid_value.which() == "ui":
                    grid_value.ui = int(mep_eval(expr, {var_name, int(grid_value.ui)}))
                return grid_value

            out_ip = fbp_capnp.IP.new_message()

            new_attrs = {}
            is_valid_to_attr = "to_attr" in config and len(config["to_attr"]) > 0
            calc = config.get("calc", {})

            # update attr
            if is_valid_to_attr:
                calc_val = update_val(calc.get("f(gv)", {}), calc.copy().pop("f(gv)"), grid_val)
                new_attrs[config["to_attr"]] = maybe_as_common_value(calc_val)

            # send via content
            if not is_valid_to_attr:
                calc_val = update_val(calc.get("f(gv)", {}), calc.copy().pop("f(gv)"), grid_val)
                out_ip.content = maybe_as_common_value(calc_val)

            # copy old attributes and potentially add new one
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **new_attrs)
            await ports["out"].write(value=out_ip)

    except Exception as e:
        print(f"{os.path.basename(__file__)} Exception :", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
