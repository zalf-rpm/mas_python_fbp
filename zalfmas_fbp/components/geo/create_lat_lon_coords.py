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
import shared
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "country_ids", "region"],
        outs=["lat_lon_coords"],
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

    country_ids_data = shared.load_grid_cached(config["path_to_country_ids_grid"], int)

    # get just default values for region and country_ids
    region = config["region"]
    country_ids = json.loads(config["country_ids"])

    # at least one input port has to be connected
    while ports["lat_lon_coords"] and (ports["region"] or ports["country_ids"]):
        try:
            if ports["region"]:
                msg = await ports["region"].read()
                if msg.which() == "done":
                    ports["region"] = None
                    continue
                else:
                    region_ip = msg.value.as_struct(fbp_capnp.IP)
                    region = region_ip.content.as_text()

            if ports["country_ids"]:
                msg = await ports["country_ids"].read()
                if msg.which() == "done":
                    ports["country_ids"] = None
                    continue
                else:
                    country_ids_ip = msg.value.as_struct(fbp_capnp.IP)
                    c_ids_txt = country_ids_ip.content.as_text()
                    if len(c_ids_txt):
                        country_ids = json.loads(c_ids_txt)
                        if isinstance(country_ids, int):
                            country_ids = [country_ids]
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

        lat_lon_bounds = region_to_lat_lon_bounds.get(config["region"])

        lats_scaled = range(
            int(lat_lon_bounds["tl"]["lat"] * s_res_scale_factor),
            int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) - 1,
            -int(s_resolution * s_res_scale_factor),
        )
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

                country_id = country_ids_data["value"](lat, lon, False)
                if not country_id or (
                    len(country_ids) > 0 and country_id not in country_ids
                ):
                    continue

                lat_lons.append([lat, lon, country_id])

        try:
            lat_lons_ip = fbp_capnp.IP.new_message(content=json.dumps(lat_lons))
            await ports["lat_lon_coords"].write(value=lat_lons_ip)
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "region": "africa",
    "setups-file": "sim_setups_africa_calibration.csv",
    "resolution": "5min",  # 30sec,
    "country_ids": [],
    "port:conf": "[TOML string] -> component configuration",
    "path_to_country_ids_grid": "data/country-id_0.083deg_4326_wgs84_africa.asc",
    "port:country_ids": None,  # [1,2,3] :string of serialized json array containing country ids
    "port:region": None,  # africa | nigeria | earth :string
    "port:lat_lon_coords": "",  # [[lat1,lon1],[lat2,lon2]] :string of serialized json array containing array of lat/lon pairs
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Copy IP to all attached array out ports"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
