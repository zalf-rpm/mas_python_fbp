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

from capnp.lib.capnp import KjException
from mas.schema.climate import climate_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "climate", "name": "Climate"},
    "component": {
        "info": {
            "id": "ce4749cc-abab-4830-9eb3-1c44c9d451ce",
            "name": "datasets -> timeseries",
            "description": "Get timeseries capabilties from a dataset.",
        },
        "type": "standard",
        "inPorts": [{"name": "ds"}],
        "outPorts": [{"name": "ts"}],
        "defaultConfig": {
            "no_of_locations_at_once": 10,
            "no_of_locations_at_once_type": "int",
            "no_of_locations_at_once_desc": "number of locations to send at once",
            "continue_after_location_id": False,
            "continue_after_location_id_type": "string",
            "continue_after_location_id_desc": "continue after a particular location id",
            "to_attr": None,
            "to_attr_type": "string",
            "to_attr_desc": "send data attached to attribute 'to_attr'",
            "create_substream": False,
            "create_substream_type": "[true | false]",
            "create_substream_desc": "create a substream for each datasets' timeseries",
            "maintain_incoming_substreams": False,
            "maintain_incoming_substreams_type": "[true | false]",
            "maintain_incoming_substreams_desc": "if false, ignore bracket IPs, thus flatten incoming substreams",
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "ds"], outs=["ts"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    while pc.in_ports["ds"] and pc.out_ports["ts"]:
        try:
            ds_msg = await pc.in_ports["ds"].read()
            if ds_msg.which() == "done":
                pc.in_ports["ds"] = None
                continue

            ds_ip = ds_msg.value.as_struct(fbp_capnp.IP)

            # pass through brackets as we just want to preserve structure for downstream components
            if ds_ip.type == "openBracket":
                if config["maintain_incoming_substreams"]:
                    await pc.out_ports["ts"].write(ds_ip)
                continue
            if ds_ip.type == "closeBracket":
                if config["maintain_incoming_substreams"]:
                    await pc.out_ports["ts"].write(ds_ip)

            dataset = None
            try:
                dataset = ds_ip.content.as_interface(climate_capnp.Dataset)
            except (KjException, TypeError):
                try:
                    dataset = (
                        dataset_cap.cast_as(climate_capnp.Dataset)
                        if (
                            dataset_cap := await pc.connection_manager.try_connect(
                                ds_ip.content.as_text(),
                                retry_secs=1,
                            )
                        )
                        is not None
                        else None
                    )
                except (KjException, RuntimeError, OSError, TypeError) as e:
                    logger.error("Error: Couldn't connect to dataset. Exception: %s", e)
                    continue
            if dataset is None:
                continue

            if config["continue_after_location_id"]:
                callback = dataset.streamLocations(config["continue_after_location_id"]).locationsCallback
            else:
                callback = dataset.streamLocations().locationsCallback
            info = await dataset.info()
            # callback = await callback_prom

            if config["create_substream"]:
                await pc.out_ports["ts"].write(value=fbp_capnp.IP.new_message(type="openBracket", content=info.id))
            while True:
                ls = (await callback.nextLocations(int(config["no_of_locations_at_once"]))).locations
                if len(ls) == 0:
                    break
                for location in ls:
                    rc = location.customData[0].value.as_struct(geo_capnp.RowCol)
                    attrs = [{"key": "id", "value": f"row-{rc.row}_col-{rc.col}"}]
                    if config["to_attr"]:
                        attrs.append({"key": config["to_attr"], "value": location.timeSeries})
                    out_ip = fbp_capnp.IP.new_message(attributes=attrs)
                    if not config["to_attr"]:
                        out_ip.content = location.timeSeries
                    await pc.out_ports["ts"].write(value=out_ip)
            if config["create_substream"]:
                await pc.out_ports["ts"].write(value=fbp_capnp.IP.new_message(type="closeBracket", content=info.id))

        except Exception:
            logger.exception("%s Exception", os.path.basename(__file__))

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


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
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
