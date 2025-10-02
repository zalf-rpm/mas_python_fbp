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
from pymep.realParser import eval as mep_eval
from zalfmas_capnp_schemas import common_capnp, fbp_capnp, geo_capnp, grid_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


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
            print(
                f"{os.path.basename(__file__)} No soil service could be received or connected to."
            )
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
            if is_valid_to_attr and config["to_attr"] in calc:
                new_attrs[config["to_attr"]] = maybe_as_common_value(
                    update_val(calc[config["to_attr"]], config["to_attr"], grid_val)
                )

            # send via content
            if not is_valid_to_attr:
                if "out" in calc:
                    grid_val = update_val(calc[config["out"]], config["out"], grid_val)
                out_ip.content = maybe_as_common_value(grid_val)

            # copy old attributes and potentially add new one
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **new_attrs)
            await ports["out"].write(value=out_ip)

    except Exception as e:
        print(f"{os.path.basename(__file__)} Exception :", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "as_common_value": False,
    "from_attr": "[string]",  # name of the attribute to get coordinate from (on "in" IP) (e.g. latlon)
    "to_attr": "[string]",  # store result on attribute with this name
    "port:conf": "[TOML string] -> component configuration",
    "port:service": "[sturdy ref | capability]",  # capability or sturdy ref to service
    "port:in": "[geo_capnp.LatLonCoord]",  # lat/lon coordinate
    "port:out": "[grid.capnp:Grid.Value | common.capnp:Value]",  # value at requested location
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Get grid value at lat/lon location, either via external Grid service or starting it within the component."
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
