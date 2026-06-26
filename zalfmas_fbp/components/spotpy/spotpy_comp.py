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
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import override

import matplotlib.pyplot as plt
import spotpy
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from numpy import floating, ndarray
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run.process.config.config_codec import config_from_ip

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    repetitions: int = Field(
        10,
        description="number of repetitions",
    )
    path_to_out_folder: str = Field(
        "out/",
        description="path to output folder",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="spotpy",
        name="Spotpy",
    ),
    info=meta.Info(
        id="09dbe4c2-c9df-46ab-a30c-239b84d5c6ab",
        name="Spotpy calibration component",
        description="The actual component in the center of the calibration flow.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="init_params",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
            desc="key/value pair description of the parameters to calibrate",
        ),
        meta.Port(
            name="obs_values",
            contentType="List[common/common_capnp:Value.lf64]",
            desc="list of observations",
        ),
        meta.Port(
            name="sim_values",
            contentType="List[common/common_capnp:Value.lf64]",
            desc="list of simulated values",
        ),
    ],
    outPorts=[
        meta.Port(
            name="sampled_params",
            contentType="List[common/common.capnp:Value.lpair(Text, common/common_capnp:Value.f64)]",
            desc="[name1: value1, name2: value2, ...] list of param_name -> sampled value pairs",
        ),
        meta.Port(
            name="best",
            contentType="Text",
            desc="best optimized result",
        ),
    ],
    config=Config,
)


class SpotPySetup:
    def __init__(
        self,
        params,
        observations,
        sampled_params_out_p,
        sim_values_in_p,
        loop: asyncio.AbstractEventLoop,
        log_out_p=None,
    ):
        self.params = params
        self.observations = observations
        self.sampled_params_out_p = sampled_params_out_p
        self.sim_values_in_p = sim_values_in_p
        self.loop = loop
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
            asyncio.run_coroutine_threadsafe(self.sampled_params_out_p.write(value=out_ip), self.loop).result()
            logger.info("%s %s sent params to monica setup: %s", Path(__file__).name, datetime.now(), vector)
            if self.log_out_p:
                asyncio.run_coroutine_threadsafe(
                    self.log_out_p.write(value={"content": f"{datetime.now()} sent params to monica setup: {vector}"}),
                    self.loop,
                ).result()

            in_msg = asyncio.run_coroutine_threadsafe(self.sim_values_in_p.read(), self.loop).result()
            # check for end of data from in port
            if in_msg.which() == "done":
                return None

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            sim_values = list(in_ip.content.as_struct(common_capnp.Value).lf64)
            if self.log_out_p:
                asyncio.run_coroutine_threadsafe(
                    self.log_out_p.write(
                        value={
                            "content": f"len(sim_values): {len(sim_values)} == len(self.observations): "
                            f"{len(self.observations)}",
                        },
                    ),
                    self.loop,
                ).result()
            assert len(sim_values) == len(self.observations)
        except Exception:
            logger.exception("%s %s exception", Path(__file__).name, datetime.now())

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


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        loop = asyncio.get_running_loop()

        while (
            self.out_ports["sampled_params"]
            and self.in_ports["sim_values"]
            and (self.in_ports["init_params"] or self.in_ports["obs_values"])
        ):
            db_dir = None
            try:
                spotpy_params: list[spotpy.parameter.Uniform] | None = None
                if self.in_ports["init_params"]:
                    try:
                        init_params_ip = await self.read_in("init_params")
                        if init_params_ip is None:
                            self.in_ports["init_params"] = None
                            continue

                        init_params = config_from_ip(init_params_ip)
                        if not init_params:
                            self.in_ports["init_params"] = None
                            continue

                        spotpy_params = []
                        for name, par in init_params.items():
                            par_name = name
                            if "array_index" in par:
                                # spotpy does not allow two parameters to have the same name
                                par_name += f"_{par['array']}"
                            spotpy_params.append(spotpy.parameter.Uniform(**par))
                        if len(spotpy_params) == 0:
                            logger.warning("%s: no parameters to calibrate!", Path(__file__).name)
                            continue

                    except Exception:
                        logger.exception("%s Exception", Path(__file__).name)
                        continue

                obs_values = None
                param_set_id = None
                if self.in_ports["obs_values"]:
                    try:
                        obs_values_ip = await self.read_in("obs_values")
                        if obs_values_ip is None:
                            self.in_ports["obs_values"] = None
                            continue

                        for attr in obs_values_ip.attributes:
                            if attr.key == "param_set_id":
                                param_set_id = attr.value.as_text()
                        obs_values = obs_values_ip.content.as_struct(common_capnp.Value).lf64
                        if not obs_values or len(obs_values) == 0:
                            logger.warning("%s: no observed values to calibrate!", Path(__file__).name)
                            continue
                    except Exception:
                        logger.exception("%s Exception", Path(__file__).name)
                        continue

                spot_setup = SpotPySetup(
                    spotpy_params,
                    obs_values,
                    self.out_ports["sampled_params"],
                    self.in_ports["sim_values"],
                    loop,
                )

                rep = self.config.repetitions
                db_dir = tempfile.TemporaryDirectory()
                path_to_spotpy_db = f"{db_dir.name}/SCEUA_results"
                # Set up the sampler with the model above
                sampler = spotpy.algorithms.sceua(spot_setup, dbname=path_to_spotpy_db, dbformat="csv")

                # Run the sampler in a thread so the event loop stays free to handle
                # the async port calls made by SpotPySetup.simulation() via
                # asyncio.run_coroutine_threadsafe().
                # ngs = number of complexes
                # kstop = max number of evolution loops before convergence
                # peps = convergence criterion
                # pcento = percent change allowed in kstop loops before convergence
                await asyncio.to_thread(sampler.sample, rep, ngs=len(spotpy_params) * 2, peps=0.001, pcento=0.001)

                if self.out_ports["best"]:
                    best_out_stream = io.StringIO()
                    print_status_final(sampler.status, best_out_stream)
                    best_ip = fbp_capnp.IP.new_message(content=best_out_stream.getvalue())
                    await self.write_out("best", best_ip)

                results: ndarray = spotpy.analyser.load_csv_results(path_to_spotpy_db)
                fig = plt.figure(1, figsize=(9, 6))
                plt.plot(results["like1"], "r+")
                plt.show()
                plt.ylabel("RMSE")
                plt.xlabel("Iteration")
                fig.savefig(
                    f"{self.config.path_to_out_folder}/{param_set_id}_SCEUA_objectivefunctiontrace_MONICA.png",
                    dpi=150,
                )
                plt.close(fig)

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

            if db_dir:
                db_dir.cleanup()

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
