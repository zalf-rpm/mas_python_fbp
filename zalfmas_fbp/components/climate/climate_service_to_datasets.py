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
from zalfmas_capnp_schemas import climate_capnp, fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "cs"], outs=["ds"]
    )
    await p.update_config_from_port(config, ports["conf"])

    while ports["cs"] and ports["ds"]:
        try:
            service = ports.read_or_connect("cs", cast_as=climate_capnp.Service)
            if service is None:
                continue

            info_prom = service.info()
            datasets = (await service.getAvailableDatasets()).datasets
            if datasets and len(datasets) == 0:
                continue

            info = await info_prom
            if config["create_substream"]:
                ports["ds"].write(
                    value=fbp_capnp.IP.new_message(type="openBracket", content=info.id)
                )
            for meta_plus_data in datasets if datasets else []:
                attrs = []
                if config["to_attr"]:
                    attrs.append(
                        {"key": config["to_attr"], "value": meta_plus_data.data}
                    )
                out_ip = fbp_capnp.IP.new_message(attributes=attrs)
                if not config["to_attr"]:
                    out_ip.content = meta_plus_data.data
                await ports["ds"].write(value=out_ip)
            if config["create_substream"]:
                ports["ds"].write(
                    value=fbp_capnp.IP.new_message(type="closeBracket", content=info.id)
                )

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": None,
    "create_substream": False,
    # "select": None,
    # "opt:from_attr": "[name: string] -> get sturdy ref or capability from attibute 'from_attr'",
    "opt:to_attr": "[name: string] -> send data attached to attribute 'to_attr'",
    # "opt:select": "[id: string (id)] -> dataset id to select",
    "opt:create_substream": "[true | false] -> create a substream for each climate services' datasets",
    "port:conf": "[TOML string] -> component configuration",
    "port:cs": "[climate_capnp.Service (capability) | climate_capnp.Service (sturdy ref)] -> receive cap to climate service",
    "port:ds": "[climate.capnp:Dataset (capability)] -> get all the datasets for the input climate service",
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
