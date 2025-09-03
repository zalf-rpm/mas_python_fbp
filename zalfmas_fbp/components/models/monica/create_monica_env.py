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
import sys
import uuid

import capnp
import zalfmas_capnp_schemas
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
sys.path.append(os.path.join(os.path.dirname(zalfmas_capnp_schemas.__file__), "model", "monica"))
import climate_capnp
import common_capnp
import fbp_capnp
import geo_capnp
import grid_capnp
import management_capnp as mgmt_capnp
import model_capnp
import sim_setup_capnp
import soil_capnp


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
    #if len(scenario) > 0 and scenario[:3].lower() == "rcp":
    #    site_json["EnvironmentParameters"]["rcp"] = scenario

    with open(crop) as _:
        crop_json = json.load(_)

    # set the current crop used for this run id
    crop_json["cropRotation"][2] = crop_id

    # create environment template from json templates
    env_template = monica_io.create_env_json_from_json_config({
        "crop": crop_json,
        "site": site_json,
        "sim": sim_json,
        "climate": ""
    })

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
            llcoord = attrs.pop(config["coord_attr"]).as_struct(geo_capnp.LatLonCoord)
            setup = attrs.pop(config["setup_attr"]).as_struct(sim_setup_capnp.Setup)

            env_template = create_env(setup.simJson, setup.cropJson, setup.siteJson, setup.cropId)

            env_template["params"]["userCropParameters"]["__enable_vernalisation_factor_fix__"] = setup.useVernalisationFix

            if config["ilr_attr"] in attrs:
                ilr = attrs.pop(config["ilr_attr"]).as_struct(mgmt_capnp.ILRDates)
                worksteps = env_template["cropRotation"][0]["worksteps"]
                sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
                if ilr._has("sowing"):
                    s = ilr.sowing
                    sowing_ws["date"] = f"{s.year:04d}-{s.month:02d}-{s.day:02d}"
                if ilr._has("earliestSowing"):
                    s = ilr.earliestSowing
                    sowing_ws["earliest-date"] = f"{s.year:04d}-{s.month:02d}-{s.day:02d}"
                if ilr._has("latestSowing"):
                    s = ilr.latestSowing
                    sowing_ws["latest-date"] = f"{s.year:04d}-{s.month:02d}-{s.day:02d}"

                harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))
                if ilr._has("harvest"):
                    h = ilr.harvest
                    harvest_ws["date"] = f"{h.year:04d}-{h.month:02d}-{h.day:02d}"
                if ilr._has("latestHarvest"):
                    h = ilr.latestHarvest
                    harvest_ws["latest-date"] = f"{h.year:04d}-{h.month:02d}-{h.day:02d}"

            env_template["params"]["userCropParameters"][
                "__enable_T_response_leaf_expansion__"
            ] = setup.leafExtensionModifier

            #print("soil:", soil_profile)
            #env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile.layers

            # setting groundwater level
            #if setup.groundwaterLevel:
            #    groundwaterlevel = 20
            #    layer_depth = 0
            #    for layer in soil_profile:
            #        if layer.get("is_in_groundwater", False):
            #            groundwaterlevel = layer_depth
            #            #print("setting groundwaterlevel of soil_id:", str(soil_id), "to", groundwaterlevel, "m")
            #            break
            #        layer_depth += get_value(layer["Thickness"])
            #    env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
            #    env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [max(0, groundwaterlevel - 0.2) , "m"]
            #    env_template["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [groundwaterlevel + 0.2, "m"]

            # setting impenetrable layer
            #if setup.impenetrableLayer:
            #    impenetrable_layer_depth = get_value(env_template["params"]["userEnvironmentParameters"]["LeachingDepth"])
            #    layer_depth = 0
            #    for layer in soil_profile:
            #        if layer.get("is_impenetrable", False):
            #            impenetrable_layer_depth = layer_depth
            #            #print("setting leaching depth of soil_id:", str(soil_id), "to", impenetrable_layer_depth, "m")
            #            break
            #        layer_depth += get_value(layer["Thickness"])
            #    env_template["params"]["userEnvironmentParameters"]["LeachingDepth"] = [impenetrable_layer_depth, "m"]
            #    env_template["params"]["siteParameters"]["ImpenetrableLayerDepth"] = [impenetrable_layer_depth, "m"]

            if setup.elevation and config["dgm_attr"] in attrs:
                height_nn = (
                    attrs.pop(config["dgm_attr"]).as_struct(grid_capnp.Grid.Value).f
                )
                env_template["params"]["siteParameters"]["heightNN"] = height_nn

            if setup.slope and config["slope_attr"] in attrs:
                slope = (
                    attrs.pop(config["slope_attr"]).as_struct(grid_capnp.Grid.Value).f
                )
                env_template["params"]["siteParameters"]["slope"] = slope / 100.0

            if setup.latitude:
                env_template["params"]["siteParameters"]["Latitude"] = llcoord.lat

            if setup.co2 > 0:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = setup.co2

            if setup.o3 > 0:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = setup.o3

            if setup.fieldConditionModifier:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = setup.fieldConditionModifier

            if len(setup.stageTemperatureSum) > 0:
                stage_ts = setup.stageTemperatureSum.split('_')
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print('The provided StageTemperatureSum array is not '
                            'sufficiently long. Falling back to original StageTemperatureSum')

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup.fertilization
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup.irrigation

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup.nitrogenResponseOn
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup.waterDeficitResponseOn
            env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup.emergenceMoistureControlOn
            env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup.emergenceFloodingControlOn

            if config["id_attr"] in attrs:
                id_ = attrs[config["id_attr"]].as_text()
            else:
                id_ = str(uuid.uuid4())
                attrs["id_attr"] = id_

            env_template["customId"] = {
                "setup_id": setup.runId,
                "id": id_,
                "crop_id": setup.cropId,
                "lat": llcoord.lat, "lon": llcoord.lon
            }

            capnp_env = model_capnp.Env.new_message()

            if config["climate_attr"] in attrs:
                timeseries = attrs.pop(config["climate_attr"]).as_interface(climate_capnp.TimeSeries)
                capnp_env.timeSeries = timeseries
            else:
                env_template["pathToClimateCSV"] = "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/dwd/csvs/germany/row-0/col-181.csv"

            if config["soil_attr"] in attrs:
                soil_profile = attrs.pop(config["soil_attr"]).as_struct(soil_capnp.Profile)
                capnp_env.soilProfile = soil_profile

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
    "sim_json": "sim_bgr.json",
    "crop_json": "crop_bgr.json",
    "site_json": "site.json",
    "dgm_attr": "dgm",
    "slope_attr": "slope",
    "climate_attr": "climate",
    "soil_attr": "soil",
    "coord_attr": "latlon",
    "setup_attr": "setup",
    "id_attr": "id",
    "ilr_attr": "ilr",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string (MONICA JSON result)] -> receive MONICA JSON result",
    "port:out": "[string (MONICA JSON env)]"
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Create a MONICA env"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))

if __name__ == "__main__":
    main()