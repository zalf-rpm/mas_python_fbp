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

import json
import os

from zalfmas_capnp_schemas_with_stubs import (
    climate_capnp,
    common_capnp,
    fbp_capnp,
    model_capnp,
    soil_capnp,
)

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "models/monica", "name": "Models/MONICA"},
    "component": {
        "info": {
            "id": "e58b7ff4-3c76-4ea2-9873-09d6923e5c75",
            "name": "Create model.capnp:Env with MONICA payload",
            "description": "Create model.capnp:Env with MONICA JSON env payload.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "climate",
                "contentType": "climate.capnp:TimeSeries | Text",
                "desc": "Climate data for MONICA simulation, either as a TimeSeries capability or a path to a CSV file.",
            },
            {
                "name": "soil",
                "contentType": "soil.capnp:Profile | Text (JSON array)",
                "desc": "Soil profile data for MONICA simulation, either as a Profile capability or a path to a JSON file containing an array of soil layers.",
            },
            {"name": "in", "type": "Text (JSON)", "desc": "MONICA env json."},
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "model.capnp:Env",
                "desc": "An Env structure with possible attached climate/soil capabilities ready to be sent to a MONICA Cap'n Proto service or component.",
            }
        ],
        "defaultConfig": {
            "from_attr": {
                "value": None,
                "type": "Text",
                "desc": "Instead of the message content, read the MONICA JSON env from this attribute.",
            },
            "to_attr": {
                "value": None,
                "type": "Text",
                "desc": "Instead of sending the ready prepared MONICA Cap'n Proto env via the message content, send it via this attribute.",
            },
            "climate_attr": {
                "value": "@climate",
                "type": "climate.capnp:TimeSeries | Text",
                "desc": "Either a capability to a time series (via @ out of attribute) or the path to a MONICA compatible climate CSV file from config value.",
            },
            "soil_attr": {
                "value": "@soil",
                "type": "soil.capnp:Profile | Text (JSON array)",
                "desc": "Either a capability to a soil profile (via @ out of attribute) or a string (JSON array) containing a MONICA soil profile description from config value.",
            },
            "id_attr": {
                "value": "@id",
                "type": "Text",
                "desc": "Id of current env via @ out of attribute or a UUID4 will be automatically generated.",
            },
        },
    },
}


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
            json_env_str, is_capnp = p.get_config_val(config, "from_attr", attrs, as_text=True, remove=True)
            if not is_capnp:
                json_env_str = in_ip.content.as_text()
            json_env = json.loads(json_env_str)

            capnp_env = model_capnp.Env.new_message()

            if ports["climate"]:
                timeseries = ports.read_or_connect("climate", cast_as=climate_capnp.TimeSeries)
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
                    json_env["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

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


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
