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
from datetime import date, timedelta

import capnp
from pyproj import CRS
from zalfmas_capnp_schemas import fbp_capnp, geo_capnp
from zalfmas_capnp_schemas import management_capnp as mgmt_capnp
from zalfmas_common import common, geo
from zalfmas_services.management import ilr_sowing_harvest_dates as ilr

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "in"],
        outs=["out"],
    )
    await p.update_config_from_port(config, ports["conf"])

    wgs84_crs = CRS.from_epsg(4326)
    utm32n_crs = CRS.from_epsg(25832)

    ilr_seed_harvest_data = {}
    for crop_id in config["crop_ids"]:
        # read seed/harvest dates for each crop_id
        path_to_csv = config["path_to_ilr_csv"].get(crop_id, None)
        if not path_to_csv:
            continue
        print("Read data and created ILR seed/harvest interpolator:", path_to_csv)
        try:
            ilr_seed_harvest_data[crop_id] = (
                ilr.read_data_and_create_seed_harvest_geo_grid_interpolator(
                    crop_id, path_to_csv, wgs84_crs, utm32n_crs
                )
            )
        except OSError:
            print("Couldn't read file:", path_to_csv)

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            latlon = common.get_fbp_attr(in_ip, config["latlon_attr"]).as_struct(
                geo_capnp.LatLonCoord
            )
            sowing_time = common.get_fbp_attr(
                in_ip, config["sowing_time_attr"]
            ).as_text()
            harvest_time = common.get_fbp_attr(
                in_ip, config["harvest_time_attr"]
            ).as_text()
            crop_id = common.get_fbp_attr(in_ip, config["crop_id_attr"]).as_text()

            utm = geo.transform_from_to_geo_coord(latlon, "utm32n")
            ilr_interpolate = ilr_seed_harvest_data[crop_id]["interpolate"]
            seed_harvest_cs = ilr_interpolate(utm.r, utm.h) if ilr_interpolate else None

            out_ip = fbp_capnp.IP.new_message()
            if ilr_interpolate is None or seed_harvest_cs is None:
                common.copy_and_set_fbp_attrs(in_ip, out_ip)
                await ports["out"].write(value=out_ip)
            else:
                ilr_dates = mgmt_capnp.ILRDates.new_message()

                seed_harvest_data = ilr_seed_harvest_data[crop_id]["data"][
                    seed_harvest_cs
                ]
                if seed_harvest_data:
                    is_winter_crop = ilr_seed_harvest_data[crop_id]["is-winter-crop"]

                    if (
                        sowing_time == "fixed"
                    ):  # fixed indicates that regionally fixed sowing dates will be used
                        sowing_date = seed_harvest_data["sowing-date"]
                    elif (
                        sowing_time == "auto"
                    ):  # auto indicates that automatic sowing dates will be used that vary between regions
                        sowing_date = seed_harvest_data["latest-sowing-date"]
                    else:
                        sowing_date = None

                    if sowing_date:
                        sds = sowing_date
                        sd = date(2001, sds["month"], sds["day"])
                        sdoy = sd.timetuple().tm_yday

                    if (
                        harvest_time == "fixed"
                    ):  # fixed indicates that regionally fixed harvest dates will be used
                        harvest_date = seed_harvest_data["harvest-date"]
                    elif (
                        harvest_time == "auto"
                    ):  # auto indicates that automatic harvest dates will be used that vary between regions
                        harvest_date = seed_harvest_data["latest-harvest-date"]
                    else:
                        harvest_date = None

                    # print("sowing_date:", ilr_dates["sowing"], "harvest_date:", ilr_dates["harvest"])

                    if harvest_date:
                        hds = harvest_date
                        hd = date(2001, hds["month"], hds["day"])
                        hdoy = hd.timetuple().tm_yday

                    esds = seed_harvest_data["earliest-sowing-date"]
                    esd = date(2001, esds["month"], esds["day"])

                    # sowing after harvest should probably never occur in both fixed setups!
                    if sowing_time == "fixed" and harvest_time == "fixed":
                        # calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=min(hdoy, sdoy - 1)
                            )
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=hdoy
                            )
                        ilr_dates.sowing = seed_harvest_data["sowing-date"]
                        ilr_dates.harvest = {
                            "year": hds["year"],
                            "month": calc_harvest_date.month,
                            "day": calc_harvest_date.day,
                        }  # "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["sowing"])
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["harvest"])

                    elif sowing_time == "fixed" and harvest_time == "auto":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=min(hdoy, sdoy - 1)
                            )
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=hdoy
                            )
                        ilr_dates.sowing = seed_harvest_data["sowing-date"]
                        ilr_dates.latestHarvest = {
                            "year": hds["year"],
                            "month": calc_harvest_date.month,
                            "day": calc_harvest_date.day,
                        }  # "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["sowing"])
                        # print("dates: ", int(seed_harvest_cs), ":", latest_harvest_date)

                    elif sowing_time == "auto" and harvest_time == "fixed":
                        ilr_dates.earliestSowing = (
                            seed_harvest_data["earliest-sowing-date"]
                            if esd > date(esd.year, 6, 20)
                            else {"year": sds["year"], "month": 6, "day": 20}
                        )  # "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        calc_sowing_date = date(2000, 12, 31) + timedelta(
                            days=max(hdoy + 1, sdoy)
                        )
                        ilr_dates.latestSowing = {
                            "year": sds["year"],
                            "month": calc_sowing_date.month,
                            "day": calc_sowing_date.day,
                        }  # "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month, calc_sowing_date.day)
                        ilr_dates.harvest = seed_harvest_data["harvest-date"]
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["earliestSowing"], "<", ilr_dates["latestSowing"])
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["harvest"])

                    elif sowing_time == "auto" and harvest_time == "auto":
                        ilr_dates.earliestSowing = (
                            seed_harvest_data["earliest-sowing-date"]
                            if esd > date(esd.year, 6, 20)
                            else {"year": sds["year"], "month": 6, "day": 20}
                        )  # "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=min(hdoy, sdoy - 1)
                            )
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(
                                days=hdoy
                            )
                        ilr_dates.latestSowing = seed_harvest_data["latest-sowing-date"]
                        ilr_dates.latestHarvest = {
                            "year": hds["year"],
                            "month": calc_harvest_date.month,
                            "day": calc_harvest_date.day,
                        }  # "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["earliestSowing"], "<", ilr_dates["latestSowing"])
                        # print("dates: ", int(seed_harvest_cs), ":", ilr_dates["latestHarvest"])

                common.copy_and_set_fbp_attrs(
                    in_ip,
                    out_ip,
                    **({config["to_attr"]: ilr_dates} if config["to_attr"] else {}),
                )
                await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "crop_ids": [
        "WW",
        "SW",
        "WB",
    ],  # ALF,CLALF,GM,PO,SB,SBee,SM,SU,SW,SWR,WB,WG_test,WR,WRa,WW
    "crop_id_attr": "cropId",
    "latlon_attr": "latlon",
    "path_to_ilr_csv": {
        "WW": "./data/management/ilr_seed_harvest_doys_germany/ILR_SEED_HARVEST_doys_WW.csv",
    },
    "sowing_time_attr": "sowingTime",  # "fixed", #fixed | auto
    "harvest_time_attr": "harvestTime",  # "fixed", #fixed | auto
    "to_attr": "ilr",  # store result on attribute with this name
    "from_attr": "[string]",  # name of the attribute to get coordinate from (on "in" IP) (e.g. latlon)
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[geo_capnp.LatLonCoord]",  # lat/lon coordinate
    "port:out": "[grid_capnp.Grid.Value]",  # value at requested location
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Get ILR seed/harvest dates at the given lat/lon location."
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
