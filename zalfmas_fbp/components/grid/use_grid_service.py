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

# remote debugging via commandline
# -m ptvsd --host 0.0.0.0 --port 14000 --wait

import asyncio
import os
import sys

import capnp
import zalfmas_capnp_schemas
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp
import geo_capnp
import grid_capnp


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

            val = service.closestValueAt(coord).wait().val

            out_ip = fbp_capnp.IP.new_message()
            if not config["to_attr"]:
                out_ip.content = val
            common.copy_and_set_fbp_attrs(
                in_ip, out_ip, **({config["to_attr"]: val} if config["to_attr"] else {})
            )
            await ports["out"].write(value=out_ip)

    except Exception as e:
        print(f"{os.path.basename(__file__)} Exception :", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

default_config = {
    "path_to_ascii_grid": None,
    "grid_crs": "utm32n",
    "val_type": "[int | float]",
    "from_attr": "[string]",  # name of the attribute to get coordinate from (on "in" IP) (e.g. latlon)
    "to_attr": "[string]", # store result on attribute with this name
    "port:conf": "[TOML string] -> component configuration",
    "port:service": "[sturdy ref | capability]", # capability or sturdy ref to service
    "port:in": "[geo_capnp.LatLonCoord]",  # lat/lon coordinate
    "port:out": "[grid_capnp.Grid.Value]",  # value at requested location
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