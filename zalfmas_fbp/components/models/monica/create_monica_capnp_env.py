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
import json
import os

import capnp
from zalfmas_capnp_schemas import (
    climate_capnp,
    common_capnp,
    fbp_capnp,
    model_capnp,
    soil_capnp,
)

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "climate", "soil", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    timeseries = None
    soil_profile = None
    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            # check for end of data from in port
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attrs = {kv.key: kv.value for kv in in_ip.attributes}
            json_env_str, is_capnp = p.get_config_val(
                config, "from_attr", attrs, as_text=True, remove=True
            )
            if not is_capnp:
                json_env_str = in_ip.content.as_text()
            json_env = json.loads(json_env_str)

            capnp_env = model_capnp.Env.new_message()

            if ports["climate"]:
                timeseries = ports.read_or_connect(
                    "climate", cast_as=climate_capnp.TimeSeries
                )
                if timeseries:
                    capnp_env.timeSeries = timeseries
            if not timeseries and "climate" in config:
                timeseries, is_capnp = p.get_config_val(
                    config,
                    "climate",
                    attrs,
                    as_interface=climate_capnp.TimeSeries,
                    remove=True,
                )
                if is_capnp:
                    capnp_env.timeSeries = timeseries
                else:
                    json_env["pathToClimateCSV"] = timeseries

            if ports["soil"]:
                soil_profile = ports.read_or_connect("soil", cast_as=soil_capnp.Profile)
                if soil_profile:
                    capnp_env.soilProfile = soil_profile
            if not soil_profile and "soil" in config:
                soil_profile, is_capnp = p.get_config_val(
                    config,
                    "soil",
                    attrs,
                    as_interface=soil_capnp.Profile,
                    remove=True,
                )
                if is_capnp:
                    capnp_env.soilProfile = soil_profile
                else:
                    json_env["params"]["siteParameters"]["SoilProfileParameters"] = (
                        soil_profile
                    )

            capnp_env.rest = common_capnp.StructuredText.new_message(
                value=json.dumps(json_env), structure={"json": None}
            )

            out_ip = common_capnp.IP.new_message()

            if "to_attr" in config and len(config["to_attr"]) > 0:
                attrs[config["to_attr"]] = capnp_env
            else:
                out_ip.content = capnp_env

            out_ip.attributes = list([{"key": k, "value": v} for k, v in attrs.items()])
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "from_attr": None,
    "to_attr": None,
    "climate": "@climate",
    "soil": "@soil",
    "port:conf": "[TOML string] -> component configuration",
    "port:soil": "[soil.capnp:Profile | sturdy ref] -> soil profile",
    "port:climate": "[climate.capnp:TimeSeries | sturdy ref] -> time series to use",
    "port:in": "[JSON string] -> MONICA env",
    "port:out": "[model.capnp:Env (with MONICA JSON env payload)]",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Create a Cap'n Proto model.capnp:Env structure with a MONICA env as payload"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
