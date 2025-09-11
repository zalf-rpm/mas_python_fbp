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
import time
from datetime import date, datetime, timedelta

import capnp
import numpy as np
from netCDF4 import Dataset
from zalfmas_capnp_schemas import common_capnp, fbp_capnp, model_capnp
from zalfmas_common import csv
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

from ..geo import get_lat_lon_grid_value as shared


def check_for_nill_dates(mgmt):
    for key, value in mgmt.items():
        if "date" in key and value == "Nill":
            return False
    return True


def mgmt_date_to_rel_date(mgmt_date):
    if mgmt_date[:5] == "0000-":
        return mgmt_date

    day_str, month_short_name = mgmt_date.split("-")
    month_str = "00"
    if month_short_name == "Jan":
        month_str = "01"
    elif month_short_name == "Feb":
        month_str = "02"
    elif month_short_name == "Mar":
        month_str = "03"
    elif month_short_name == "Apr":
        month_str = "04"
    elif month_short_name == "May":
        month_str = "05"
    elif month_short_name == "Jun":
        month_str = "06"
    elif month_short_name == "Jul":
        month_str = "07"
    elif month_short_name == "Aug":
        month_str = "08"
    elif month_short_name == "Sep":
        month_str = "09"
    elif month_short_name == "Oct":
        month_str = "10"
    elif month_short_name == "Nov":
        month_str = "11"
    elif month_short_name == "Dec":
        month_str = "12"

    return f"0000-{month_str}-{int(day_str):02}"


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "coords", "region", "params"], outs=["env"]
    )
    await p.update_config_from_port(config, ports["conf"])

    PATHS = {
        # adjust the local path to your environment
        "mbm-local-local": {
            "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
            # mounted path to archive or hard drive with climate data
            # "path-to-soil-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
            "path-to-soil-dir": "/home/berg/Desktop/soil/",
            "monica-path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
            # mounted path to archive accessable by monica executable
            "path-to-data-dir": os.path.join(
                config["path_to_repo"], "data/"
            ),  # mounted path to archive or hard drive with data
            "path-debug-write-folder": "./debug-out/",
        },
        "mbm-local-remote": {
            "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
            # mounted path to archive or hard drive with climate data
            # "path-to-soil-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
            "path-to-soil-dir": "/home/berg/Desktop/soil/",
            "monica-path-to-climate-dir": "/monica_data/climate-data/",
            # mounted path to archive accessable by monica executable
            "path-to-data-dir": "../data/",  # mounted path to archive or hard drive with data
            "path-debug-write-folder": "./debug-out/",
        },
        "hpc-local-remote": {
            # "path-to-climate-dir": "/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
            # mounted path to archive or hard drive with climate data
            "path-to-soil-dir": "/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
            "monica-path-to-climate-dir": "/monica_data/climate-data/",
            # mounted path to archive accessable by monica executable
            "path-to-data-dir": os.path.join(
                config["path_to_repo"], "data/"
            ),  # mounted path to archive or hard drive with data
            "path-debug-write-folder": "./debug-out/",
        },
    }

    path_to_out_file = config["path_to_out"] + "/producer.out"
    if not os.path.exists(config["path_to_out"]):
        try:
            os.makedirs(config["path_to_out"])
        except OSError:
            print(
                "run-calibration-producer.py: Couldn't create dir:",
                config["path_to_out"],
                "!",
            )
    with open(path_to_out_file, "a") as _:
        _.write(f"config: {config}\n")

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

    # select paths
    paths = PATHS[config["mode"]]

    # read setup from csv file
    setups = csv.read_csv(config["setups-file"], key="run-id")
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    # open netcdfs
    path_to_soil_netcdfs = paths["path-to-soil-dir"] + "/" + config["resolution"] + "/"
    if config["resolution"] == "5min":
        soil_data = {
            "sand": {
                "var": "SAND",
                "file": "SAND5min.nc",
                "conv_factor": 0.01,
            },  # % -> fraction
            "clay": {
                "var": "CLAY",
                "file": "CLAY5min.nc",
                "conv_factor": 0.01,
            },  # % -> fraction
            "corg": {
                "var": "OC",
                "file": "OC5min.nc",
                "conv_factor": 0.01,
            },  # scale factor
            "bd": {
                "var": "BD",
                "file": "BD5min.nc",
                "conv_factor": 0.01 * 1000.0,
            },  # scale factor * 1 g/cm3 = 1000 kg/m3
        }
    else:
        soil_data = None  # ["Sand5min.nc", "Clay5min.nc", "OC5min.nc", "BD5min.nc"]
    soil_datasets = {}
    soil_vars = {}
    for elem, data in soil_data.items():
        ds = Dataset(path_to_soil_netcdfs + data["file"], "r", format="NETCDF4")
        soil_datasets[elem] = ds
        soil_vars[elem] = ds.variables[data["var"]]

    def create_soil_profile(row, col):
        # skip first 4.5cm layer and just use 7 layers
        layers = []

        layer_depth = 8
        # find the fill value for the soil data
        for elem2 in soil_data.keys():
            for i in range(8):
                if np.ma.is_masked(soil_vars[elem2][i, row, col]):
                    if i < layer_depth:
                        layer_depth = i
                    break
        layer_depth -= 1

        if layer_depth < 4:
            return None

        for i, real_depth_cm, monica_depth_m in [
            (0, 4.5, 0),
            (1, 9.1, 0.1),
            (2, 16.6, 0.1),
            (3, 28.9, 0.1),
            (4, 49.3, 0.2),
            (5, 82.9, 0.3),
            (6, 138.3, 0.6),
            (7, 229.6, 0.7),
        ][1:]:
            if i <= layer_depth:
                layers.append(
                    {
                        "Thickness": [monica_depth_m, "m"],
                        "SoilOrganicCarbon": [
                            soil_vars["corg"][i, row, col]
                            * soil_data["corg"]["conv_factor"],
                            "%",
                        ],
                        "SoilBulkDensity": [
                            soil_vars["bd"][i, row, col]
                            * soil_data["bd"]["conv_factor"],
                            "kg m-3",
                        ],
                        "Sand": [
                            soil_vars["sand"][i, row, col]
                            * soil_data["sand"]["conv_factor"],
                            "fraction",
                        ],
                        "Clay": [
                            soil_vars["clay"][i, row, col]
                            * soil_data["clay"]["conv_factor"],
                            "fraction",
                        ],
                    }
                )
        return layers

    setup = None
    if len(run_setups) > 1 and run_setups[0] not in setups:
        print("More than one setup given or given setup not in list of setups.")
        exit(1)
    else:
        setup_id = run_setups[0]
        setup = setups[setup_id]
    assert setup is not None

    region = setup["region"] if "region" in setup else config["region"]
    coords = []
    params = {}

    start_component_time = time.perf_counter()
    # as long as we get parameters, we create the envs
    while ports["env"] and ports["params"] and (ports["coords"] or ports["region"]):
        sent_env_count = 0
        try:
            if ports["region"]:
                msg = ports["region"].read().wait()
                if msg.which() == "done":
                    ports["region"] = None
                else:
                    region_ip = msg.value.as_struct(fbp_capnp.IP)
                    region = region_ip.content.as_text()

            # read the coordinates
            if ports["coords"]:
                msg = ports["coords"].read().wait()
                if msg.which() == "done":
                    ports["coords"] = None
                else:
                    coords_ip = msg.value.as_struct(fbp_capnp.IP)
                    coords = json.loads(coords_ip.content.as_text())

            # read the parameters to be calibrated
            if ports["params"]:
                msg = ports["params"].read().wait()
                if msg.which() == "done":
                    ports["params"] = None
                    continue
                else:
                    params_ip = msg.value.as_struct(fbp_capnp.IP)
                    params = json.loads(params_ip.content.as_text())
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)
            continue

        start_setup_time = time.perf_counter()

        gcm = setup["gcm"]
        scenario = setup["scenario"]
        ensmem = setup["ensmem"]
        crop = setup["crop"]

        lat_lon_bounds = region_to_lat_lon_bounds.get(region)

        if setup["region"] == "nigeria":
            planting = setup["planting"].lower()
            nitrogen = setup["nitrogen"].lower()
            management_file = f"{planting}_planting_{nitrogen}_nitrogen.csv"
            # load management data
            management = csv.read_csv(
                paths["path-to-data-dir"]
                + "/agro_ecological_regions_nigeria/"
                + management_file,
                key="id",
            )
        else:
            planting = nitrogen = management = None

        eco_data = shared.load_grid_cached(
            paths["path-to-data-dir"]
            + "/agro_ecological_regions_nigeria/agro-eco-regions_0.038deg_4326_wgs84_nigeria.asc",
            int,
        )
        crop_mask_data = shared.load_grid_cached(
            paths["path-to-data-dir"]
            + f"/{setup['crop']}-mask_0.083deg_4326_wgs84_africa.asc.gz",
            int,
        )
        planting_data = shared.load_grid_cached(
            paths["path-to-data-dir"]
            + f"/{setup['crop']}-planting-doy_0.5deg_4326_wgs84_africa.asc",
            int,
        )
        harvest_data = shared.load_grid_cached(
            paths["path-to-data-dir"]
            + f"/{setup['crop']}-harvest-doy_0.5deg_4326_wgs84_africa.asc",
            int,
        )
        height_data = shared.load_grid_cached(
            paths["path-to-data-dir"] + "/../" + setup["path_to_dem_asc_grid"], float
        )
        slope_data = shared.load_grid_cached(
            paths["path-to-data-dir"] + "/../" + setup["path_to_slope_asc_grid"], float
        )

        # read template sim.json
        with open(
            os.path.join(
                config["path_to_repo"], setup.get("sim.json", config["sim.json"])
            )
        ) as _:
            sim_json = json.load(_)
        # change start and end date according to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            end_year = int(setup["end_date"].split("-")[0])
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])
        sim_json["include-file-base-path"] = os.path.join(
            config["path_to_repo"], sim_json["include-file-base-path"]
        )

        # read template site.json
        with open(
            os.path.join(
                config["path_to_repo"], setup.get("site.json", config["site.json"])
            )
        ) as _:
            site_json = json.load(_)

        if len(scenario) > 0 and scenario[:3].lower() == "ssp":
            site_json["EnvironmentParameters"]["rcp"] = f"rcp{scenario[-2:]}"

        # read template crop.json
        with open(
            os.path.join(
                config["path_to_repo"], setup.get("crop.json", config["crop.json"])
            )
        ) as _:
            crop_json = json.load(_)
            # set current crop
            for ws in crop_json["cropRotation"][0]["worksteps"]:
                if "Sowing" in ws["type"]:
                    ws["crop"][2] = crop
            # set value of calibration params
            ps = crop_json["crops"][crop]["cropParams"]
            for pname, pval in params.items():
                if pname in ps["species"]:
                    ps["species"][pname] = pval
                elif pname in ps["cultivar"]:
                    ps["cultivar"][pname] = pval

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = (
            setup["use_vernalisation_fix"]
            if "use_vernalisation_fix" in setup
            else False
        )

        # create environment template from json templates
        env_template = monica_io.create_env_json_from_json_config(
            {"crop": crop_json, "site": site_json, "sim": sim_json, "climate": ""}
        )

        c_lon_0 = -179.75
        c_lat_0 = +89.25
        c_resolution = 0.5

        s_lat_0 = region_to_lat_lon_bounds["earth"][config["resolution"]]["tl"]["lat"]
        s_lon_0 = region_to_lat_lon_bounds["earth"][config["resolution"]]["tl"]["lon"]

        for lat, lon, country_id in coords:
            c_col = int((lon - c_lon_0) / c_resolution)
            c_row = int((c_lat_0 - lat) / c_resolution)

            s_col = int((lon - s_lon_0) / s_resolution)
            s_row = int((s_lat_0 - lat) / s_resolution)

            # set management
            mgmt = None
            aer = None
            if region == "nigeria":
                aer = eco_data["value"](lat, lon, False)
                if aer and aer > 0 and aer in management:
                    mgmt = management[aer]
            else:
                mgmt = {}
                planting_doy = planting_data["value"](lat, lon, False)
                if planting_doy:
                    d = date(2023, 1, 1) + timedelta(days=planting_doy - 1)
                    mgmt["Sowing date"] = f"0000-{d.month:02}-{d.day:02}"
                harvest_doy = harvest_data["value"](lat, lon, False)
                if harvest_doy:
                    d = date(2023, 1, 1) + timedelta(days=harvest_doy - 1)
                    mgmt["Harvest date"] = f"0000-{d.month:02}-{d.day:02}"

            valid_mgmt = False
            if mgmt and shared.check_for_nill_dates(mgmt) and len(mgmt) > 1:
                valid_mgmt = True
                for ws in env_template["cropRotation"][0]["worksteps"]:
                    if ws["type"] == "Sowing" and "Sowing date" in mgmt:
                        ws["date"] = shared.mgmt_date_to_rel_date(mgmt["Sowing date"])
                        if "Planting density" in mgmt:
                            ws["PlantDensity"] = [
                                float(mgmt["Planting density"]),
                                "plants/m2",
                            ]
                    elif ws["type"] == "Harvest" and "Harvest date" in mgmt:
                        ws["date"] = shared.mgmt_date_to_rel_date(mgmt["Harvest date"])
                    elif ws["type"] == "AutomaticHarvest" and "Harvest date" in mgmt:
                        ws["latest-date"] = shared.mgmt_date_to_rel_date(
                            mgmt["Harvest date"]
                        )
                    elif ws["type"] == "Tillage" and "Tillage date" in mgmt:
                        ws["date"] = shared.mgmt_date_to_rel_date(mgmt["Tillage date"])
                    elif (
                        ws["type"] == "MineralFertilization"
                        and mgmt[:2] == "N "
                        and mgmt[-5:] == " date"
                    ):
                        app_no = int(ws["application"])
                        app_str = str(app_no) + ["st", "nd", "rd", "th"][app_no - 1]
                        ws["date"] = shared.mgmt_date_to_rel_date(
                            mgmt[f"N {app_str} date"]
                        )
                        ws["amount"] = [
                            float(mgmt[f"N {app_str} application (kg/ha)"]),
                            "kg",
                        ]
            else:
                mgmt = None
            if not mgmt or not valid_mgmt:
                continue

            crop_mask_value = crop_mask_data["value"](lat, lon, False)
            if not crop_mask_value or crop_mask_value == 0:
                continue

            height_nn = height_data["value"](lat, lon, False)
            if not height_nn:
                continue

            slope = slope_data["value"](lat, lon, False)
            if not slope:
                slope = 0

            soil_profile = create_soil_profile(s_row, s_col)
            if not soil_profile or len(soil_profile) == 0:
                continue

            env_template["params"]["userCropParameters"][
                "__enable_T_response_leaf_expansion__"
            ] = setup["LeafExtensionModifier"]

            env_template["params"]["siteParameters"]["SoilProfileParameters"] = (
                soil_profile
            )

            if setup["elevation"]:
                env_template["params"]["siteParameters"]["heightNN"] = height_nn

            if setup["slope"]:
                if setup["slope_unit"] == "degree":
                    s = slope / 90.0
                else:
                    s = slope
                env_template["params"]["siteParameters"]["slope"] = s

            if setup["latitude"]:
                env_template["params"]["siteParameters"]["Latitude"] = lat

            if setup["FieldConditionModifier"]:
                for ws in env_template["cropRotation"][0]["worksteps"]:
                    if "Sowing" in ws["type"]:
                        if "|" in setup["FieldConditionModifier"] and aer and aer > 0:
                            fcms = setup["FieldConditionModifier"].split("|")
                            fcm = float(fcms[aer - 1])
                            if fcm > 0:
                                ws["crop"]["cropParams"]["species"][
                                    "FieldConditionModifier"
                                ] = fcm
                        else:
                            ws["crop"]["cropParams"]["species"][
                                "FieldConditionModifier"
                            ] = setup["FieldConditionModifier"]

            env_template["params"]["simulationParameters"][
                "UseNMinMineralFertilisingMethod"
            ] = setup["fertilization"]
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = (
                setup["irrigation"]
            )
            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = (
                setup["NitrogenResponseOn"]
            )
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = (
                setup["WaterDeficitResponseOn"]
            )
            env_template["params"]["simulationParameters"][
                "EmergenceMoistureControlOn"
            ] = setup["EmergenceMoistureControlOn"]
            env_template["params"]["simulationParameters"][
                "EmergenceFloodingControlOn"
            ] = setup["EmergenceFloodingControlOn"]

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
            hist_sub_path = f"isimip/3b_v1.1_CMIP6/csvs/{gcm}/historical/{ensmem}/row-{c_row}/col-{c_col}.csv.gz"
            sub_path = f"isimip/3b_v1.1_CMIP6/csvs/{gcm}/{scenario}/{ensmem}/row-{c_row}/col-{c_col}.csv.gz"
            if setup["incl_historical"] and scenario != "historical":
                climate_data_paths = [
                    paths["monica-path-to-climate-dir"] + hist_sub_path,
                    paths["monica-path-to-climate-dir"] + sub_path,
                ]
            else:
                climate_data_paths = [paths["monica-path-to-climate-dir"] + sub_path]
            env_template["pathToClimateCSV"] = climate_data_paths
            # print("pathToClimateCSV:", env_template["pathToClimateCSV"])

            env_template["customId"] = {
                "lat": lat,
                "lon": lon,
                "env_id": sent_env_count + 1,
                "nodata": False,
                "country_id": country_id,
            }

            try:
                await ports["env"].write(
                    value=fbp_capnp.IP.new_message(
                        content=model_capnp.Env.new_message(
                            rest=common_capnp.StructuredText.new_message(
                                value=json.dumps(env_template), structure={"json": None}
                            )
                        )
                    )
                )
            except Exception as e:
                print(f"{os.path.basename(__file__)} Exception:", e)
                continue

            sent_env_count += 1

        # send a last message will be just forwarded by monica to signify last
        if env_template:
            env_template["pathToClimateCSV"] = ""
            env_template["customId"] = {
                "no_of_sent_envs": sent_env_count,
                "nodata": True,
            }
            try:
                await ports["env"].write(
                    value=fbp_capnp.IP.new_message(
                        content=model_capnp.Env.new_message(
                            rest=common_capnp.StructuredText.new_message(
                                value=json.dumps(env_template), structure={"json": None}
                            )
                        )
                    )
                )
            except Exception as e:
                print(f"{os.path.basename(__file__)} Exception:", e)
                continue

        stop_setup_time = time.perf_counter()
        print_str = f"{os.path.basename(__file__)}: {datetime.now()} Sending {sent_env_count} envs took {stop_setup_time - start_setup_time} seconds\n"
        print(print_str)
        with open(path_to_out_file, "a") as _:
            _.write(print_str)

    stop_component_time = time.perf_counter()
    print_str = f"{os.path.basename(__file__)}: {datetime.now()} Running component took {stop_component_time - start_component_time} seconds\n"
    print(print_str)
    with open(path_to_out_file, "a") as _:
        _.write(print_str)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "mode": "mbm-local-local",
    "path_to_sim.json": "sim.json",
    "path_to_crop.json": "crop.json",
    "path_to_site.json": "site.json",
    "region": "africa",
    "setups-file": "sim_setups_africa_calibration.csv",
    "path_to_repo": "./",
    "path_to_data": "data/",
    "path_to_out": "out/",
    "run-setups": "[1]",
    "resolution": "5min",  # 30sec,
    "port:conf": "[TOML string] -> component configuration",
    "port:coords": None,  # [lat,lon,country_id] :string json serialized array of array
    "port:region": None,  # africa | nigeria | earth :string
    "port:params": None,  # {} :string json serialized object of param_name -> value
    "port:env": "",  # stream of strings (json serialized MONICA env)
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "calibration producer for africa"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
