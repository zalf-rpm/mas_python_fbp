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
import uuid

import capnp
from zalfmas_capnp_schemas import (
    climate_capnp,
    common_capnp,
    fbp_capnp,
    geo_capnp,
    grid_capnp,
    model_capnp,
    sim_setup_capnp,
    soil_capnp,
)
from zalfmas_capnp_schemas import (
    management_capnp as mgmt_capnp,
)
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


def create_env(sim, crop, site, crop_id):
    if not hasattr(create_env, "cache"):
        create_env.cache = {}

    scsc = (sim, crop, site, crop_id)

    if scsc in create_env.cache:
        return create_env.cache[scsc]

    with open(sim) as _:
        sim_json = json.load(_)

    with open(site) as _:
        site_json = json.load(_)
    # if len(scenario) > 0 and scenario[:3].lower() == "rcp":
    #    site_json["EnvironmentParameters"]["rcp"] = scenario

    with open(crop) as _:
        crop_json = json.load(_)

    # set the current crop used for this run id
    crop_json["cropRotation"][2] = crop_id

    # create environment template from json templates
    env_template = monica_io.create_env_json_from_json_config(
        {"crop": crop_json, "site": site_json, "sim": sim_json, "climate": ""}
    )

    env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

    create_env.cache[scsc] = env_template
    return env_template


def get_value(list_or_value):
    return list_or_value[0] if isinstance(list_or_value, list) else list_or_value


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            # check for end of data from in port
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attrs = {kv.key: kv.value for kv in in_ip.attributes}
            if "coord" in config:
                ll_coord, is_capnp = p.get_config_val(
                    config, "dgm", attrs, as_struct=geo_capnp.LatLonCoord, remove=True
                )
                ll_coord = (
                    ll_coord
                    if is_capnp
                    else geo_capnp.LatLonCoord.new_message(**ll_coord)
                )
            else:
                continue

            if "setup" in config:
                setup, is_capnp = p.get_config_val(
                    config, "dgm", attrs, as_struct=sim_setup_capnp.Setup, remove=True
                )
                setup = (
                    setup
                    if is_capnp
                    else sim_setup_capnp.Setup.new_message(**config["setup"])
                )
            else:
                continue

            env_template = create_env(
                setup.simJson, setup.cropJson, setup.siteJson, setup.cropId
            )

            env_template["params"]["userCropParameters"][
                "__enable_vernalisation_factor_fix__"
            ] = setup.useVernalisationFix

            if config["ilr"] in attrs:
                ilr = attrs.pop(config["ilr"]).as_struct(mgmt_capnp.ILRDates)
                worksteps = env_template["cropRotation"][0]["worksteps"]
                sowing_ws = next(
                    filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps)
                )
                if ilr._has("sowing"):
                    s = ilr.sowing
                    sowing_ws["date"] = f"{s.year:04d}-{s.month:02d}-{s.day:02d}"
                if ilr._has("earliestSowing"):
                    s = ilr.earliestSowing
                    sowing_ws["earliest-date"] = (
                        f"{s.year:04d}-{s.month:02d}-{s.day:02d}"
                    )
                if ilr._has("latestSowing"):
                    s = ilr.latestSowing
                    sowing_ws["latest-date"] = f"{s.year:04d}-{s.month:02d}-{s.day:02d}"

                harvest_ws = next(
                    filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps)
                )
                if ilr._has("harvest"):
                    h = ilr.harvest
                    harvest_ws["date"] = f"{h.year:04d}-{h.month:02d}-{h.day:02d}"
                if ilr._has("latestHarvest"):
                    h = ilr.latestHarvest
                    harvest_ws["latest-date"] = (
                        f"{h.year:04d}-{h.month:02d}-{h.day:02d}"
                    )

            env_template["params"]["userCropParameters"][
                "__enable_T_response_leaf_expansion__"
            ] = setup.leafExtensionModifier

            # print("soil:", soil_profile)
            # env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile.layers

            if setup.elevation and "dgm" in config:
                height_nn, is_capnp = p.get_config_val(
                    config, "dgm", attrs, as_struct=grid_capnp.Grid.Value, remove=True
                )
                env_template["params"]["siteParameters"]["heightNN"] = (
                    height_nn.f if is_capnp else height_nn
                )

            if setup.slope and "slope" in config:
                slope, is_capnp = p.get_config_val(
                    config, "slope", attrs, as_struct=grid_capnp.Grid.Value, remove=True
                )
                env_template["params"]["siteParameters"]["slope"] = (
                    slope.f if is_capnp else slope
                ) / 100.0

            if setup.latitude:
                env_template["params"]["siteParameters"]["Latitude"] = ll_coord.lat

            if setup.co2 > 0:
                env_template["params"]["userEnvironmentParameters"][
                    "AtmosphericCO2"
                ] = setup.co2

            if setup.o3 > 0:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = (
                    setup.o3
                )

            if setup.fieldConditionModifier:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"][
                    "species"
                ]["FieldConditionModifier"] = setup.fieldConditionModifier

            if len(setup.stageTemperatureSum) > 0:
                stage_ts = setup.stageTemperatureSum.split("_")
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"][
                    "cropParams"
                ]["cultivar"]["StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print(
                        "The provided StageTemperatureSum array is not "
                        "sufficiently long. Falling back to original StageTemperatureSum"
                    )

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"][
                    "cultivar"
                ]["StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"][
                "UseNMinMineralFertilisingMethod"
            ] = setup.fertilization
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = (
                setup.irrigation
            )

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = (
                setup.nitrogenResponseOn
            )
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = (
                setup.waterDeficitResponseOn
            )
            env_template["params"]["simulationParameters"][
                "EmergenceMoistureControlOn"
            ] = setup.emergenceMoistureControlOn
            env_template["params"]["simulationParameters"][
                "EmergenceFloodingControlOn"
            ] = setup.emergenceFloodingControlOn

            if "id" in config:
                id_, is_capnp = p.get_config_val(
                    config, "id", attrs, as_text=True, remove=False
                )
            else:
                id_ = str(uuid.uuid4())
                attrs["id"] = id_

            env_template["customId"] = {
                "setup_id": setup.runId,
                "id": id_,
                "crop_id": setup.cropId,
                "lat": ll_coord.lat,
                "lon": ll_coord.lon,
            }

            capnp_env = model_capnp.Env.new_message()

            if "climate" in config:
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
                    env_template["pathToClimateCSV"] = timeseries

            if "soil" in config:
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
                    env_template["params"]["siteParameters"][
                        "SoilProfileParameters"
                    ] = soil_profile

            capnp_env.rest = common_capnp.StructuredText.new_message(
                value=json.dumps(env_template), structure={"json": None}
            )
            out_ip = common_capnp.IP.new_message(
                content=capnp_env,
                attributes=list([{"key": k, "value": v} for k, v in attrs.items()]),
            )
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "sim_json": "@sim.json",
    "crop_json": "@crop.json",
    "site_json": "@site.json",
    "dgm": "@dgm",
    "slope": "@slope",
    "climate": "@climate",
    "soil": "@soil",
    "coord": "@latlon",
    "setup": "@setup",
    "id": "@id",
    "ilr": "@ilr",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string (MONICA JSON result)] -> receive MONICA JSON result",
    "port:out": "[string (MONICA JSON env)]",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Create a MONICA env")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
