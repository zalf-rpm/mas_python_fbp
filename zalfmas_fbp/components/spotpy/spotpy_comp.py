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
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime

import capnp
import matplotlib.pyplot as plt
import spotpy
import zalfmas_capnp_schemas

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp


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
        msg_content = dict(zip(vector.name, vector))
        out_ip = fbp_capnp.IP.new_message(content=json.dumps(msg_content))
        self.sampled_params_out_p.write(value=out_ip).wait()
        print(
            f"{os.path.basename(__file__)} {datetime.now()} sent params to monica setup: {vector}"
        )
        if self.log_out_p:
            self.log_out_p.write(
                value={
                    "content": f"{datetime.now()} sent params to monica setup: {vector}"
                }
            ).wait()

        msg = self.sim_values_in_p.read().wait()
        # check for end of data from in port
        if msg.which() == "done":
            return

        in_ip = msg.value.as_struct(fbp_capnp.IP)
        s: str = in_ip.content.as_text()
        sim_values = json.loads(s)
        if self.log_out_p:
            self.log_out_p.write(
                value={
                    "content": f"len(sim_values): {len(sim_values)} == len(self.observations): "
                    f"{len(self.observations)}"
                }
            ).wait()
        assert len(sim_values) == len(self.observations)
        return sim_values

    def evaluation(self):
        return self.observations

    def objectivefunction(self, simulation, evaluation):
        return spotpy.objectivefunctions.rmse(evaluation, simulation)


def print_status_final(sampler_status, stream):
    print("\n*** Final SPOTPY summary ***")
    print(
        "Total Duration: "
        + str(round((time.time() - sampler_status.starttime), 2))
        + " seconds",
        file=stream,
    )
    print("Total Repetitions:", sampler_status.rep, file=stream)

    if sampler_status.optimization_direction == "minimize":
        print(
            "Minimal objective value: %g" % (sampler_status.objectivefunction_min),
            file=stream,
        )
        print("Corresponding parameter setting:", file=stream)
        for i in range(sampler_status.parameters):
            text = "%s: %g" % (sampler_status.parnames[i], sampler_status.params_min[i])
            print(text, file=stream)

    if sampler_status.optimization_direction == "maximize":
        print(
            "Maximal objective value: %g" % (sampler_status.objectivefunction_max),
            file=stream,
        )
        print("Corresponding parameter setting:", file=stream)
        for i in range(sampler_status.parameters):
            text = "%s: %g" % (sampler_status.parnames[i], sampler_status.params_max[i])
            print(text, file=stream)

    if sampler_status.optimization_direction == "grid":
        print(
            "Minimal objective value: %g" % (sampler_status.objectivefunction_min),
            file=stream,
        )
        print("Corresponding parameter setting:", file=stream)
        for i in range(sampler_status.parameters):
            text = "%s: %g" % (sampler_status.parnames[i], sampler_status.params_min[i])
            print(text, file=stream)

        print(
            "Maximal objective value: %g" % (sampler_status.objectivefunction_max),
            file=stream,
        )
        print("Corresponding parameter setting:", file=stream)
        for i in range(sampler_status.parameters):
            text = "%s: %g" % (sampler_status.parnames[i], sampler_status.params_max[i])
            print(text, file=stream)

    print("******************************\n", file=stream)


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["config", "init_params", "obs_values", "sim_values"],
        outs=["sampled_params", "best"],
    )
    await p.update_config_from_port(config, ports["conf"])

    while (
        ports["sampled_params"]
        and ports["sim_values"]
        and (ports["init_params"] or ports["obs_values"])
    ):
        db_dir = None
        try:
            spotpy_params = None
            if ports["init_params"]:
                try:
                    msg = await ports["init_params"].read()
                    if msg.which() == "done":
                        ports["init_params"] = None
                        continue

                    init_params_ip = msg.value.as_struct(fbp_capnp.IP)
                    init_params = json.loads(init_params_ip.content.as_text())
                    user_params = init_params

                    spotpy_params = []
                    if len(user_params) > 0:
                        for par in user_params:
                            par_name = par["name"]
                            if "array" in par:
                                if re.search(
                                    r"\d", par["array"]
                                ):  # check if par["array"] contains numbers
                                    par_name += (
                                        "_" + par["array"]
                                    )  # spotpy does not allow two parameters to have the same name
                            if (
                                "derive_function" not in par
                            ):  # spotpy does not care about derived params
                                spotpy_params.append(spotpy.parameter.Uniform(**par))
                    if len(spotpy_params) == 0:
                        print(
                            f"{os.path.basename(__file__)}: no parameters to calibrate!"
                        )
                        continue

                except Exception as e:
                    print(f"{os.path.basename(__file__)} Exception:", e)
                    continue

            obs_values = None
            param_set_id = None
            if ports["obs_values"]:
                try:
                    msg = await ports["obs_values"].read()
                    # check for end of data from in port
                    if msg.which() == "done":
                        ports["obs_values"] = None
                        continue

                    obs_values_ip = msg.value.as_struct(fbp_capnp.IP)
                    for attr in obs_values_ip.attributes:
                        if attr.key == "param_set_id":
                            param_set_id = attr.value.as_test()
                    obs_values = json.loads(obs_values_ip.content.as_text())
                    if not obs_values or len(obs_values) == 0:
                        print(
                            f"{os.path.basename(__file__)}: no observed values to calibrate!"
                        )
                        continue
                except Exception as e:
                    print(f"{os.path.basename(__file__)} Exception:", e)
                    continue

            spot_setup = SpotPySetup(
                spotpy_params, obs_values, ports["sampled_params"], ports["sim_values"]
            )

            rep = int(config["repetitions"])  # initial number was 10
            db_dir = tempfile.TemporaryDirectory()
            path_to_spotpy_db = f"{db_dir.name}/SCEUA_results"
            # Set up the sampler with the model above
            sampler = spotpy.algorithms.sceua(
                spot_setup, dbname=path_to_spotpy_db, dbformat="csv"
            )

            # Run the sampler to produce the parameter distribution
            # and identify optimal parameters based on objective function
            # ngs = number of complexes
            # kstop = max number of evolution loops before convergence
            # peps = convergence criterion
            # pcento = percent change allowed in kstop loops before convergence
            sampler.sample(rep, ngs=len(spotpy_params) * 2, peps=0.001, pcento=0.001)

            if ports["best"]:
                best_out_stream = io.StringIO()
                print_status_final(sampler.status, best_out_stream)
                best_ip = fbp_capnp.IP.new_message(content=best_out_stream.getvalue())
                await ports["best"].write(value=best_ip)

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

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

        if db_dir:
            db_dir.cleanup()

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "repetitions": "10",
    "path_to_out_folder": "out/",
    # "init_algo_in_sr": None,  #
    "port:conf": "[TOML string] -> component configuration",
    "port:init_params": None,  # some value struct
    "port:obs_values": None,  # [obs_value1, obs_value2, ...] :string - json serialization of list of observations
    "port:sim_values": None,  # [sim_value1, sim_value2, ...] :string - json serialized list of simulated values
    "port:sampled_params": None,  # {name1: value1, name2: value2, ...} :string of json serialized object of param_name -> sampled value
    "port:best": None,  # string of best optimized result
    # "port:log_out_sr": None,  # output info messages
}


def main():
    parser = c.create_default_fbp_component_args_parser("Spotpy calibration component")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
