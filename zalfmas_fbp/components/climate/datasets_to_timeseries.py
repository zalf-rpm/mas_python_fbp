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
from zalfmas_capnp_schemas import climate_capnp, fbp_capnp, geo_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "ds"], outs=["ts"]
    )
    await p.update_config_from_port(config, ports["conf"])

    while ports["ds"] and ports["ts"]:
        try:
            ds_msg = await ports["ds"].read()
            if ds_msg.which() == "done":
                ports["ds"] = None
                continue

            ds_ip = ds_msg.value.as_struct(fbp_capnp.IP)

            # pass through brackets as we just want to preserve structure for downstream components
            if ds_ip.type == "openBracket":
                if config["maintain_incoming_substreams"]:
                    await ports["ts"].write(ds_ip)
                continue
            elif ds_ip.type == "closeBracket":
                if config["maintain_incoming_substreams"]:
                    await ports["ts"].write(ds_ip)

            dataset = None
            try:
                dataset = ds_ip.content.as_interface(climate_capnp.Dataset)
            except Exception:
                try:
                    dataset = ports.connection_manager.try_connect(
                        ds_ip.content.as_text(),
                        cast_as=climate_capnp.Dataset,
                        retry_secs=1,
                    )
                except Exception as e:
                    print("Error: Couldn't connect to dataset. Exception:", e)
                    continue
            if dataset is None:
                continue

            if config["continue_after_location_id"]:
                callback = dataset.streamLocations(
                    config["continue_after_location_id"]
                ).locationsCallback
            else:
                callback = dataset.streamLocations().locationsCallback
            info = await dataset.info()
            # callback = await callback_prom

            if config["create_substream"]:
                await ports["ts"].write(
                    value=fbp_capnp.IP.new_message(type="openBracket", content=info.id)
                )
            while True:
                ls = (
                    await callback.nextLocations(int(config["no_of_locations_at_once"]))
                ).locations
                if len(ls) == 0:
                    break
                for l in ls:
                    rc = l.customData[0].value.as_struct(geo_capnp.RowCol)
                    attrs = [{"key": "id", "value": f"row-{rc.row}_col-{rc.col}"}]
                    if config["to_attr"]:
                        attrs.append({"key": config["to_attr"], "value": l.timeSeries})
                    out_ip = fbp_capnp.IP.new_message(attributes=attrs)
                    if not config["to_attr"]:
                        out_ip.content = l.timeSeries
                    await ports["ts"].write(value=out_ip)
            if config["create_substream"]:
                await ports["ts"].write(
                    value=fbp_capnp.IP.new_message(type="closeBracket", content=info.id)
                )

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "no_of_locations_at_once": "10",
    "continue_after_location_id": None,
    "to_attr": None,
    "create_substream": False,
    "maintain_incoming_substreams": False,
    "opt:no_of_locations_at_once": "[int] -> number of locations to send at once",
    "opt:continue_after_location_id": "[string] -> continue after a particular location id",
    # "opt:from_attr": "[name:string] -> get sturdy ref or capability from attibute 'from_attr'",
    "opt:to_attr": "[name:string] -> send data attached to attribute 'to_attr'",
    "opt:create_substream": "[true | false] -> create a substream for each datasets' timeseries",
    "opt:maintain_incoming_substreams": "[true | false] -> if false, ignore bracket IPs thus flatten incoming substreams",
    "port:conf": "[TOML string] -> component configuration",
    "port:ds": "[climate_capnp.Dataset]-> ",
    "port:ts": "[climate.capnp:TimeSeries (capability)] -> get all the timeseries for the input climate dataset",
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
