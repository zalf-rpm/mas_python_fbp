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

from zalfmas_capnp_schemas_with_stubs import fbp_capnp, geo_capnp, common_capnp
from zalfmas_common import rect_ascii_grid_management as grid

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "geo",
        "name": "Geo"
    },
    "component": {
        "info": {
            "id": "1229ed4f-9fef-4b76-9061-a117d52e9bc2",
            "name": "create lat lon coords",
            "description": "Create lat/lon coords for region."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "ids",
                "contentType": "List[int]",
                "desc": "List of IDs to include in the output."
            }, {
                "name": "region",
                "contentType": "string",
                "desc": "The region we create the coords for."
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "common.capnp:Pair(ID, geo.capnp:LatLonCoord) | string (JSON array)",
                "desc": "Either a stream of LatLonCoords or a serialized JSON array [[lat1,lon1],[lat2,lon2]] of lat/lon pairs."
            }
        ],
        "defaultConfig": {
            "stream": {
                "value": False,
                "type": "bool",
                "desc": "If True, the component will stream the output Lat/Lon coord by Lat/Lon coord."
            },
            "create_substream": {
                "value": False,
                "type": "bool",
                "desc": "If true, creates for each set of region and ids, a new substream."
            },
            "region": {
                "value": "africa",
                "type": ["nigeria", "africa", "earth"]
            },
            "resolution": {
                "value": "5min",
                "type": ["5min", "30sec"],
                "desc": "Select the resolution of the generated data."
            },
            "ids": {
                "value": [],
                "type": "list[int]",
                "desc": "List of IDs to include in the output, unless the 'ids' port is connected."
            },
            "path_to_ids_grid": {
                "value": "data/country-id_0.083deg_4326_wgs84_africa.asc",
                "type": "string",
                "desc": "Path to the ids grid file."
            },
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "ids", "region"],
        outs=["out"],
    )
    await p.update_config_from_port(config, ports["conf"])

    s_resolution = {"5min": 5 / 60.0, "30sec": 30 / 3600.0}[config["resolution"]]
    s_res_scale_factor = {"5min": 60.0, "30sec": 3600.0}[config["resolution"]]

    region_to_lat_lon_bounds = {
        "nigeria": {"tl": {"lat": 14.0, "lon": 2.7}, "br": {"lat": 4.25, "lon": 14.7}},
        "africa": {
            "tl": {"lat": 37.4, "lon": -17.55},
            "br": {"lat": -34.9, "lon": 51.5},
        },
        "earth": {
            "5min": {
                "tl": {"lat": 83.95833588, "lon": -179.95832825},
                "br": {"lat": -55.95833206, "lon": 179.50000000},
            },
            "30sec": {
                "tl": {"lat": 83.99578094, "lon": -179.99583435},
                "br": {"lat": -55.99583435, "lon": 179.99568176},
            },
        },
    }

    ids_grid = grid.load_grid_cached(config["path_to_ids_grid"], int)

    # get just default values for region and ids
    region = config["region"]
    ids = json.loads(config["ids"])
    do_stream = config["stream"]
    create_substream = config["create_substream"]

    # at least one input port has to be connected
    while ports["out"] and (ports["region"] or ports["ids"]):
        try:
            if ports["region"]:
                msg = await ports["region"].read()
                if msg.which() == "done":
                    ports["region"] = None
                    continue
                else:
                    region_ip = msg.value.as_struct(fbp_capnp.IP)
                    region = region_ip.content.as_text()

            if ports["ids"]:
                msg = await ports["ids"].read()
                if msg.which() == "done":
                    ports["ids"] = None
                    continue
                else:
                    ids_ip = msg.value.as_struct(fbp_capnp.IP)
                    ids = list(ids_ip.content.as_list())
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

        lat_lon_bounds = region_to_lat_lon_bounds.get(region)

        lats_scaled = range(
            int(lat_lon_bounds["tl"]["lat"] * s_res_scale_factor),
            int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) - 1,
            -int(s_resolution * s_res_scale_factor),
        )

        if do_stream and create_substream:
            await ports["out"].write(value=fbp_capnp.IP.new_message(type="openBracket"))
        lat_lons = []
        for lat_scaled in lats_scaled:
            lat = lat_scaled / s_res_scale_factor
            print(str(round(lat, 2)), end=" ", flush=True)

            lons_scaled = range(
                int(lat_lon_bounds["tl"]["lon"] * s_res_scale_factor),
                int(lat_lon_bounds["br"]["lon"] * s_res_scale_factor) + 1,
                int(s_resolution * s_res_scale_factor),
            )
            for lon_scaled in lons_scaled:
                lon = lon_scaled / s_res_scale_factor

                id = ids_grid["value"](lat, lon, False)
                if not id or len(ids) > 0 and id not in ids:
                    continue

                if do_stream:
                    id_and_ll = common_capnp.Pair.new_message(fst=common_capnp.Value.new_message(i64=id),
                                                              snd=geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon))
                    out_ip = fbp_capnp.IP.new_message(content=id_and_ll)
                    await ports["out"].write(value=out_ip)
                else:
                    lat_lons.append([lat, lon, id])

        if do_stream:
            if create_substream:
                await ports["out"].write(value=fbp_capnp.IP.new_message(type="closeBracket"))
        else:
            try:
                out_ip = fbp_capnp.IP.new_message(content=json.dumps(lat_lons))
                await ports["out"].write(value=out_ip)
            except Exception as e:
                print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
