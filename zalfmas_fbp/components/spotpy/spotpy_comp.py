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
import io
import logging
import os
import tempfile
import time
from datetime import datetime

import matplotlib.pyplot as plt
import spotpy
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "spotpy", "name": "Spotpy"},
    "component": {
        "info": {
            "id": "09dbe4c2-c9df-46ab-a30c-239b84d5c6ab",
            "name": "Spotpy calibration component",
            "description": "The actual component in the center of the calibration flow.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "init_params",
                "contentType": "common.capnp:StructuredText[JSON | TOML]",
                "desc": "key/value pair description of the parameters to calibrate",
            },
            {"name": "obs_values", "contentType": "List[common_capnp.Value.lf64]", "desc": "list of observations"},
            {"name": "sim_values", "contentType": "List[common_capnp.Value.lf64]", "desc": "list of simulated values"},
        ],
        "outPorts": [
            {
                "name": "sampled_params",
                "contentType": "List[common.capnp:Value.lpair(Text, common_capnp.Value.f64)]",
                "desc": "[name1: value1, name2: value2, ...] list of param_name -> sampled value pairs",
            },
            {"name": "best", "contentType": "string", "desc": "best optimized result"},
        ],
        "defaultConfig": {
            "repetitions": {"value": 10, "type": "int", "desc": "number of repetitions"},
            "path_to_out_folder": {"value": "out/", "type": "string", "desc": "path to output folder"},
        },
    },
}


class SpotPySetup:
    def __init__(
        self,
        params,
        observations,
        sampled_params_out_p,
        sim_values_in_p,
        log_out_p=None,
    ):
        self.params = params
        self.observations = observations
        self.sampled_params_out_p = sampled_params_out_p
        self.sim_values_in_p = sim_values_in_p
        self.log_out_p = log_out_p

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        # vector = MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate
        sim_values = None
        try:
            name_to_param = dict(zip(vector.name, vector))
            out_msg_value = common_capnp.Value.new_message()
            n2p_list = out_msg_value.init("lpair", len(name_to_param))
            for i, (k, v) in enumerate(name_to_param.items()):
                n2p_list[i].fst = k
                n2p_list[i].snd = common_capnp.Value.new_message(f64=v)
            out_ip = fbp_capnp.IP.new_message(content=n2p_list)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.sampled_params_out_p.write(value=out_ip))
            logger.info("%s %s sent params to monica setup: %s", os.path.basename(__file__), datetime.now(), vector)
            if self.log_out_p:
                loop.run_until_complete(
                    self.log_out_p.write(value={"content": f"{datetime.now()} sent params to monica setup: {vector}"})
                )

            in_msg = loop.run_until_complete(self.sim_values_in_p.read())
            # check for end of data from in port
            if in_msg.which() == "done":
                return None

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            sim_values = list(in_ip.content.as_struct(common_capnp.Value).lf64)
            if self.log_out_p:
                loop.run_until_complete(
                    self.log_out_p.write(
                        value={
                            "content": f"len(sim_values): {len(sim_values)} == len(self.observations): "
                            f"{len(self.observations)}"
                        }
                    )
                )
            assert len(sim_values) == len(self.observations)
        except Exception:
            logger.exception("%s %s exception", os.path.basename(__file__), datetime.now())

        return sim_values

    def evaluation(self):
        return self.observations

    def objectivefunction(self, simulation, evaluation):
        return spotpy.objectivefunctions.rmse(evaluation, simulation)


def print_status_final(sampler_status, stream):
    stream.write("\n*** Final SPOTPY summary ***\n")
    stream.write(f"Total Duration: {round((time.time() - sampler_status.starttime), 2)} seconds\n")
    stream.write(f"Total Repetitions: {sampler_status.rep}\n")

    if sampler_status.optimization_direction == "minimize":
        stream.write(f"Minimal objective value: {sampler_status.objectivefunction_min:g}\n")
        stream.write("Corresponding parameter setting:\n")
        for i in range(sampler_status.parameters):
            text = f"{sampler_status.parnames[i]}: {sampler_status.params_min[i]:g}"
            stream.write(f"{text}\n")

    if sampler_status.optimization_direction == "maximize":
        stream.write(f"Maximal objective value: {sampler_status.objectivefunction_max:g}\n")
        stream.write("Corresponding parameter setting:\n")
        for i in range(sampler_status.parameters):
            text = f"{sampler_status.parnames[i]}: {sampler_status.params_max[i]:g}"
            stream.write(f"{text}\n")

    if sampler_status.optimization_direction == "grid":
        stream.write(f"Minimal objective value: {sampler_status.objectivefunction_min:g}\n")
        stream.write("Corresponding parameter setting:\n")
        for i in range(sampler_status.parameters):
            text = f"{sampler_status.parnames[i]}: {sampler_status.params_min[i]:g}"
            stream.write(f"{text}\n")

        stream.write(f"Maximal objective value: {sampler_status.objectivefunction_max:g}\n")
        stream.write("Corresponding parameter setting:\n")
        for i in range(sampler_status.parameters):
            text = f"{sampler_status.parnames[i]}: {sampler_status.params_max[i]:g}"
            stream.write(f"{text}\n")

    stream.write("******************************\n\n")


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "init_params", "obs_values", "sim_values"],
        outs=["sampled_params", "best"],
    )
    await p.update_config_from_port(config, pc.in_ports["conf"])

    while (
        pc.out_ports["sampled_params"]
        and pc.in_ports["sim_values"]
        and (pc.in_ports["init_params"] or pc.in_ports["obs_values"])
    ):
        db_dir = None
        try:
            spotpy_params = None
            if pc.in_ports["init_params"]:
                try:
                    init_params = await p.update_config_from_port({}, pc.in_ports["init_params"])
                    if not init_params:
                        pc.in_ports["init_params"] = None
                        continue

                    spotpy_params = []
                    for name, par in init_params:
                        par_name = name
                        if "array_index" in par:
                            # spotpy does not allow two parameters to have the same name
                            par_name += f"_{par['array']}"
                        spotpy_params.append(spotpy.parameter.Uniform(**par))
                    if len(spotpy_params) == 0:
                        logger.warning("%s: no parameters to calibrate!", os.path.basename(__file__))
                        continue

                except Exception:
                    logger.exception("%s Exception", os.path.basename(__file__))
                    continue

            obs_values = None
            param_set_id = None
            if pc.in_ports["obs_values"]:
                try:
                    msg = await pc.in_ports["obs_values"].read()
                    # check for end of data from in port
                    if msg.which() == "done":
                        pc.in_ports["obs_values"] = None
                        continue

                    obs_values_ip = msg.value.as_struct(fbp_capnp.IP)
                    for attr in obs_values_ip.attributes:
                        if attr.key == "param_set_id":
                            param_set_id = attr.value.as_text()
                    obs_values = obs_values_ip.content.as_struct(common_capnp.Value).lf64
                    if not obs_values or len(obs_values) == 0:
                        logger.warning("%s: no observed values to calibrate!", os.path.basename(__file__))
                        continue
                except Exception:
                    logger.exception("%s Exception", os.path.basename(__file__))
                    continue

            spot_setup = SpotPySetup(
                spotpy_params, obs_values, pc.out_ports["sampled_params"], pc.in_ports["sim_values"]
            )

            rep = config["repetitions"]  # initial number was 10
            db_dir = tempfile.TemporaryDirectory()
            path_to_spotpy_db = f"{db_dir.name}/SCEUA_results"
            # Set up the sampler with the model above
            sampler = spotpy.algorithms.sceua(spot_setup, dbname=path_to_spotpy_db, dbformat="csv")

            # Run the sampler to produce the parameter distribution
            # and identify optimal parameters based on objective function
            # ngs = number of complexes
            # kstop = max number of evolution loops before convergence
            # peps = convergence criterion
            # pcento = percent change allowed in kstop loops before convergence
            sampler.sample(rep, ngs=len(spotpy_params) * 2, peps=0.001, pcento=0.001)

            if pc.out_ports["best"]:
                best_out_stream = io.StringIO()
                print_status_final(sampler.status, best_out_stream)
                best_ip = fbp_capnp.IP.new_message(content=best_out_stream.getvalue())
                await pc.out_ports["best"].write(value=best_ip)

            results = spotpy.analyser.load_csv_results(path_to_spotpy_db)
            # Plot how the objective function was minimized during sampling
            # font = {"family": "calibri",
            #        "weight": "normal",
            #        "size": 18}
            fig = plt.figure(1, figsize=(9, 6))
            # plt.plot(results["like1"],  marker='o')
            plt.plot(results["like1"], "r+")
            plt.show()
            plt.ylabel("RMSE")
            plt.xlabel("Iteration")
            fig.savefig(
                f"{config['path_to_out_folder']}/{param_set_id}_SCEUA_objectivefunctiontrace_MONICA.png",
                dpi=150,
            )
            plt.close(fig)

        except Exception:
            logger.exception("%s Exception", os.path.basename(__file__))

        if db_dir:
            db_dir.cleanup()

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
