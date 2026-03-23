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

from zalfmas_capnp_schemas_with_stubs import fbp_capnp, geo_capnp, soil_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "soil",
        "name": "Soil"
    },
    "component": {
        "info": {
            "id": "89da0cb9-2079-4245-aecc-068194bc1637",
            "name": "Use soil service",
            "description": "Use the soil service to get the soil profiles at a given Lat/Lon coord."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "latlon",
                "contentType": "geo.capnp.LatLonCoord",
                "desc": "Lat/Lon coordinate"
            }, {
                "name": "service",
                "contentType": "soil.capnp:Service | Text (SturdyRef)",
                "desc": "Capability or sturdy ref to service."
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "grid.capnp:Grid.Value",
                "desc": "value at requested location"
            }
        ],
        "defaultConfig": {
            "from_attr": {
                "value": None,
                "type": "string",
                "desc": "Get a [geo.capnp.LatLonCoord] from the attribute 'from_attr' received on 'in' message."
            },
            "to_attr": {
                "value": None,
                "type": "string",
                "desc": "Stores the result, a [grid.capnp.Grid:Value], to attribute 'to_attr' on 'out' message."
            },
            "mandatory": {
                "value": ["soilType", "organicCarbon", "rawDensity"],
                "type": [
                    "soilType",
                    "sand",
                    "clay",
                    "silt",
                    "organicCarbon",
                    "organicMatter",
                    "rawDensity",
                    "bulkDensity",
                    "fieldCapacity",
                    "permanentWiltingPoint",
                    "saturation",
                    "sceleton",
                    "pH"
                ],
                "desc": "Which soil attributes are needed in the result to be valid?"
            },
            "optional": {
                "value": [],
                "type": [
                    "soilType",
                    "sand",
                    "clay",
                    "silt",
                    "organicCarbon",
                    "organicMatter",
                    "rawDensity",
                    "bulkDensity",
                    "fieldCapacity",
                    "permanentWiltingPoint",
                    "saturation",
                    "sceleton",
                    "pH"
                ],
                "desc": "Which soil attributes are needed in the result to be valid?"
            },
            "only_raw_data": {
                "value": False,
                "type": "bool",
                "desc": "Just return data which are physically available from the data source. If false, data can be generated from the raw data to allow more params to be available mandatory"
            },
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "latlon", "service"],
        outs=["out"],
    )
    await p.update_config_from_port(config, ports["conf"])

    service = None
    if ports["service"]:
        service = ports.read_or_connect("service", cast_as=soil_capnp.Service)
        if not service:
            print(
                f"{os.path.basename(__file__)} No soil service could be received or connected to."
            )
            return

    mandatory = config["mandatory"]
    optional = config["optional"]
    while ports["latlon"] and ports["out"] and service:
        try:
            in_msg = await ports["latlon"].read()
            if in_msg.which() == "done":
                ports["latlon"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            if attr:
                coord = attr.as_struct(geo_capnp.LatLonCoord)
            else:
                coord = in_ip.content.as_struct(geo_capnp.LatLonCoord)

            profiles = await service.profilesAt(
                coord, {"mandatory": mandatory, "optional": optional, "onlyRawData": config["only_raw_data"]}
            ).profiles
            if len(profiles) > 0:
                profile = profiles[0]

                out_ip = fbp_capnp.IP.new_message()
                if not config["to_attr"]:
                    out_ip.content = profile
                common.copy_and_set_fbp_attrs(
                    in_ip,
                    out_ip,
                    **({config["to_attr"]: profile} if config["to_attr"] else {}),
                )
                await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception :", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
