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
import logging
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol, override

import matplotlib.pyplot as plt
import numpy as np
import spotpy
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from numpy import ndarray
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

type SpotpyAlgorithm = Literal[
    "ABC",  # Artificial Bee Colony
    "DDS",  # Dynamically Dimensioned Search algorithm
    "DE-MC_Z",  # Differential Evolution Markov Chain
    "DREAM",  # DiffeRential Evolution Adaptive Metropolis
    "eFAST",  # extended Fourier Amplitude Sensitivity Test (algorithm adapted from FAST R package)
    "FAST",  # Fourier Amplitude Sensitivity Test
    "FSCABC",  # Fitness Scaling Artificial Bee Colony
    "LHS",  # Latin Hypercube Sampling
    "MC",  # Monte Carlo
    "MCMC",  # Metropolis Markov Chain Monte Carlo
    "MLE",  # Maximum Likelihood Estimation
    # "NSGA-II",  # A Fast and Elitist Multiobjective Genetic Algorithm: NSGA-II
    "PADDS",  # Pareto Archived - Dynamicallly Dimensioned Search algorithm
    "ROPE",  # RObust Parameter Estimation
    "SA",  # Simulated annealing
    "SCE-UA",  # Shuffled Complex Evolution
    "MORRIS",  # Morris Screening Sensitivity Test
]


class Config(process.ProcessConfig):
    path_to_out_folder: str = Field(
        "out/",
        description="path to output folder",
    )
    param_set_id: str = Field(
        "no-param-set-id-given",
        description="""id for the parameterset to calibrate against.
        Might be set as attribute 'param_set_id' on observed values IP received on 'obs_values' port.""",
    )
    path_to_db_dir: str | None = Field(
        None,
        description="""If set path to directory where the calibration database will be created.
        If None, then a temporary directory will be created.""",
    )
    algorithm: SpotpyAlgorithm = Field("SCE-UA", description="""SPOTPY algorithm to use""")
    repetitions: int | None = Field(
        10,
        description="""
        Maximum number of function evaluations allowed during optimization.
        ROPE: (default: None) Number of runs overall, is only used if the user does
        not specify the other arguments, otherwise its overwritten.""",
    )

    # SCE-UA
    ngs: int | None = Field(
        None,
        description="""SCE-UA: (default: 20) Number of complexes (sub-populations), take more than the number of analysed parameters.
        If None, then will be calculated as the 2*'the number of parameters'.""",
    )
    peps: dict[str, float] = Field(
        {"SCE-UA": 0.001, "ABC": 0.0001, "FSCABC": 0.0001},
        description="""
        SCE-UA: (default: 0.0000001) Value of the normalized geometric range of the parameters in the population below which convergence is deemed achieved.
        ABC: (default: 0.0001) mutation factor.
        FSCABC: (default: 0.0001) convergence criterion
        """,
    )
    pcento: float = Field(
        0.001,
        description="""SCE-UA: (default: 0.0000001) The percentage change allowed in the past kstop loops below which convergence is assumed to be achieved.""",
    )
    kstop: int = Field(
        100,
        description="""SCE-UA: (default: 100) The number of past evolution loops and their respective objective value to assess whether the marginal improvement at the current loop (in percentage) is less than pcento.""",
    )
    max_loop_inc: int | None = Field(
        None, description="""SCE-UA: (default: None) Number of loops executed at max in this function call."""
    )

    # ABC
    eb: dict[str, int] = Field(
        {"ABC": 48, "FSCABC": 48},
        description="""
        ABC: (default: 48) number of employed bees (half of population size)
        FSCABC: (default: 48) number of employed bees (half of population size)""",
    )
    a: dict[str, float] = Field(
        {"ABC": 1 / 10, "FSCABC": 1 / 10},
        description="""
        ABC: (default: 1/10) mutation factor
        FSCABC: (default: 1/10) mutation factor""",
    )
    ownlimit: bool = Field(
        False, description="""ABC: (default: false) determines if an userdefined limit is set or not."""
    )

    # DDS
    limit: dict[str, int | None] = Field(
        {"DDS": 24, "FSCABC": None},
        description="""
        DDS: (default: 24) sets the limit
        FSCABC: (default: None) sets the limit for scout bee phase""",
    )
    trials: dict[str, int] = Field(
        {"DDS": 1, "PADDS": 1},
        description="""
        DDS: (default: 1) amount of runs DDS algorithm will be performed.
        PADDS (default: 1)""",
    )
    x_initial: list[float] = Field(
        [],
        description="""DDS: (default: []) set an initial trial set as a first parameter configuration. If the set is empty the algorithm select an own initial parameter configuration.""",
    )

    # DE-MC_Z
    nChains: dict[str, int] = Field(
        {"DE-MC_Z": 3, "DREAM": 7, "MCMC": 1},
        description="""
        DE-MC_Z: (default: 3) number of different chains to employ.
        DREAM: (default: 7)
        MCMC: (default: 1)""",
    )
    burnIn: int = Field(
        100,
        description="""DE-MC_Z: (default: 100) number of iterations (meaning draws / nChains) to do before doing actual sampling.""",
    )
    thin: int = Field(1, description="""DE-MC_Z: (default: 1)""")
    convergenceCriteria: float = Field(0.8, description="""DE-MC_Z: (default: 0.8) """)
    variables_of_interest: list | None = Field(None, description="""DE-MC_Z: (default: None)""")
    DEpairs: int = Field(2, description="""DE-MC_Z: (default: 1) number of pairs of chains to base movements off of.""")
    adaptationRate: str = Field("auto", description="""DE-MC_Z: (default: auto)""")
    eps: dict[str, float] = Field(
        {"DE-MC_Z": 5e-2, "DREAM": 10e-6},
        description="""
        DE-MC_Z: (default: 5e-2) used in jittering the chains.
        DREAM: (default: 10e-6)""",
    )
    mConvergence: bool = Field(True, description="""DE-MC_Z: (default: true) """)
    mAccept: bool = Field(True, description="""DE-MC_Z: (default: true)""")

    # DREAM
    nCr: int = Field(3, description="""DREAM: (default: 3)""")
    delta: int = Field(3, description="""DREAM: (default: 3)""")
    c: float = Field(0.1, description="""DREAM: (default: 0.1)""")
    convergence_limit: float = Field(1.2, description="""DREAM: (default: 1.2)""")
    runs_after_convergence: int = Field(100, description="""DREAM: (default: 100)""")
    acceptance_test_option: int = Field(6, description="""DREAM: (default: 6)""")

    # eFAST
    freq: str = Field(
        "cukier",
        description="""eFAST: (default: cukier) indicates weather to use the frequencies after 'cukier' or McRae 'mcrae'.""",
    )
    logscale: bool | list[bool] | None = Field(
        None,
        description="""eFAST: (default: None (= np.nan)) array containing bool values indicating weather a parameter is varied
    on a logarithmic scale. In that case minimum and maximum are exponents.""",
    )

    # FAST
    M: int = Field(4, description="""FAST: (default: 4)""")

    # FSCABC
    kpow: float = Field(4, description="""FSCABC: (default: 4) exponent for power scaling method.""")

    # PADDS
    initial_objs: list = Field([], description="""PADDS: (default: [])""")
    initial_params: list = Field([], description="""PADDS: (default: [])""")
    metric: str = Field("ones", description="""PADDS: (default: ones)""")

    # ROPE
    repetitions_first_run: int | None = Field(
        None, description="""ROPE: (default: None) Number of runs in the first rune."""
    )
    subsets: int = Field(
        5,
        description="""ROPE: (default: 5) number of time the rope algorithm creates a smaller search windows for parameters.""",
    )
    percentage_first_run: float = Field(
        0.10,
        description="""ROPE: (default: 0.1) amount of runs that will be used for the next step after the first subset""",
    )
    percentage_following_runs: float = Field(
        0.10,
        description="""ROPE: (default: 0.1) amount of runs that will be used for the next step after in all following subsets.""",
    )
    NDIR: int | None = Field(None, description="""ROPE: (default: None) The number of samples to draw.""")

    # SA
    Tini: int = Field(80, description="""SA: (default: 80)""")
    Ntemp: int = Field(50, description="""SA: (default: 50)""")
    alpha: float = Field(0.99, description="""SA: (default: 0.99)""")

    # MORRIS
    num_levels: int = Field(4, description="""MORRIS: (default: 4)""")


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
            contentType="Text (JSON list)",
            desc="list of JSON objects describing the parameters to calibrate",
        ),
        meta.Port(
            name="obs_values",
            contentType="@0xe17592335373b246 = common/common_capnp:Value.lf64",
            desc="""List of observations. If the list contains null or NaN sentinel values, these will be replaced with NaN.
            These sentinel values can be given as 'null_sentinel' and 'NaN_sentinel' attributes.""",
        ),
        meta.Port(
            name="sim_values",
            contentType="@0xe17592335373b246 = common/common_capnp:Value.lf64",
            desc="""List of simulated values. If the list contains null or NaN sentinel values, these will be replaced with NaN.
            These sentinel values can be given as 'null_sentinel' and 'NaN_sentinel' attributes.""",
        ),
    ],
    outPorts=[
        meta.Port(
            name="sampled_params",
            contentType="@0xe17592335373b246 = common/common.capnp:Value.lpair[Text, common/common_capnp:Value.f64]",
            desc="[Value.pair(name1, value1), Value.pair(name2, value2), ...] list of param_name -> sampled value pairs",
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

    def _run_on_loop(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    async def _write_sampled_params(self, out_ip):
        await self.sampled_params_out_p.write(value=out_ip)

    async def _write_log(self, text: str):
        if self.log_out_p:
            await self.log_out_p.write(value={"content": text})

    async def _read_sim_values(self):
        return await self.sim_values_in_p.read()

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        # vector = MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate
        sim_values = None
        try:
            name_to_param = dict(zip(vector.name, vector))
            out_value = common_capnp.Value.new_message()
            n2p_list = out_value.init("lpair", len(name_to_param))
            for i, (k, v) in enumerate(name_to_param.items()):
                n2p_list[i].fst = k
                n2p_list[i].snd = common_capnp.Value.new_message(f64=float(v))
            out_ip = fbp_capnp.IP.new_message(content=out_value)
            self._run_on_loop(self._write_sampled_params(out_ip))
            logger.info("%s %s sent params to monica setup: %s", Path(__file__).name, datetime.now(), vector)
            if self.log_out_p:
                self._run_on_loop(self._write_log(f"{datetime.now()} sent params to monica setup: {vector}"))

            in_msg = self._run_on_loop(self._read_sim_values())
            # check for end of data from in port
            if in_msg.which() == "done":
                return None

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            sentinel_values = {}
            check_and_possibly_add_sentinel_value(sentinel_values, in_ip.attributes, "nan_sentinel")
            sim_values = capnp_value_lf64_to_numpy_array_with_nan(
                in_ip.content.as_struct(common_capnp.Value).lf64, sentinel_values=sentinel_values
            )
            if self.log_out_p:
                self._run_on_loop(
                    self._write_log(
                        f"len(sim_values): {len(sim_values)} == len(self.observations): {len(self.observations)}",
                    ),
                )
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


class SpotpyAlgo(Protocol):
    status: Any

    def sample(self, *args, **kwargs) -> Any | None: ...


def instantiate_algorithm(algo: str, spot_setup, path_to_spotpy_db, db_format="csv") -> SpotpyAlgo | None:
    if algo == "ABC":
        return spotpy.algorithms.abc(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "DDS":
        return spotpy.algorithms.dds(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "DE-MC_Z":
        return spotpy.algorithms.demcz(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "DREAM":
        return spotpy.algorithms.dream(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "eFAST":
        return spotpy.algorithms.efast(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "FAST":
        return spotpy.algorithms.fast(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "FSCABC":
        return spotpy.algorithms.fscabc(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "LHS":
        return spotpy.algorithms.lhs(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "MC":
        return spotpy.algorithms.mc(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "MCMC":
        return spotpy.algorithms.mcmc(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "MLE":
        return spotpy.algorithms.mle(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    # elif algo == "NSGA-II":
    # return spotpy.algorithms.nsgaii(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "PADDS":
        return spotpy.algorithms.padds(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "ROPE":
        return spotpy.algorithms.rope(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "SA":
        return spotpy.algorithms.sa(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "SCE-UA":
        return spotpy.algorithms.sceua(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    elif algo == "MORRIS":
        return spotpy.algorithms.morris(spot_setup, dbname=path_to_spotpy_db, dbformat=db_format)
    return None


def extract_keys_into_dict(algo: str, config: Config, keys: list[str]) -> dict[str, Any]:
    return {
        k: v[algo] if isinstance(v := config.__getattribute__(k), dict) else v
        for k in Config.model_fields.keys()
        if k in keys
    }


def sample_params_for_algorithm(algo: str, config: Config) -> dict[str, Any]:
    if algo == "ABC":
        return extract_keys_into_dict(algo, config, ["peps", "eb", "a", "ownlimit", "max_loop_inc"])
    elif algo == "DDS":
        return extract_keys_into_dict(algo, config, ["limit", "trials", "x_initial"])
    elif algo == "DE-MC_Z":
        return extract_keys_into_dict(
            algo,
            config,
            [
                "nChains",
                "burnIn",
                "thin",
                "convergenceCriteria",
                "variables_of_interest",
                "DEpairs",
                "adaptationRate",
                "eps",
                "mConvergence",
                "mAccept",
            ],
        )
    elif algo == "DREAM":
        return extract_keys_into_dict(
            algo,
            config,
            [
                "nChains",
                "eps",
                "nCr",
                "delta",
                "c",
                "convergence_limit",
                "runs_after_convergence, acceptance_test_option",
            ],
        )
    elif algo == "eFAST":
        return extract_keys_into_dict(algo, config, ["freq", "logscale"])
    elif algo == "FAST":
        return extract_keys_into_dict(algo, config, ["M"])
    elif algo == "FSCABC":
        return extract_keys_into_dict(algo, config, ["peps", "eb", "a", "limit", "kpow"])
    elif algo == "LHS":
        return {}
    elif algo == "MC":
        return {}
    elif algo == "MCMC":
        return extract_keys_into_dict(algo, config, ["nChains"])
    elif algo == "MLE":
        return {}
    # elif algo == "NSGA-II":
    # return {}
    elif algo == "PADDS":
        return extract_keys_into_dict(algo, config, ["trials", "initial_objs", "initial_params", "metric"])
    elif algo == "ROPE":
        return extract_keys_into_dict(
            algo,
            config,
            ["repetitions_first_run", "subsets", "percentage_first_run", "percentage_following_runs", "NDIR"],
        )
    elif algo == "SA":
        return extract_keys_into_dict(algo, config, ["Tini", "Ntemp", "alpha"])
    elif algo == "SCE-UA":
        return extract_keys_into_dict(algo, config, ["ngs", "peps", "pcento", "max_loop_inc"])
    elif algo == "MORRIS":
        return extract_keys_into_dict(algo, config, ["num_levels"])
    return {}


def custom_update_sample_params(algo: str, sample_params: dict[str, Any], spotpy_params: list):
    if algo == "SCE-UA":
        if sample_params.get("ngs", None) is None:
            sample_params["ngs"] = len(spotpy_params) * 2


def capnp_value_lf64_to_numpy_array(lf64: common_capnp.types.readers.Float64ListReader):
    return np.array(lf64, np.float64)


def capnp_value_lf64_to_numpy_array_with_nan(lf64: common_capnp.types.readers.Float64ListReader, sentinel_values={}):
    return np.array([sentinel_values.get(v, v) if sentinel_values else v for v in lf64])


def check_and_possibly_add_sentinel_value(sentinel_values: dict, attr, sentinel_attr_name):
    if attr.key == sentinel_attr_name:
        try:
            if (val := attr.value.as_struct(common_capnp.Value)).which() == "f64":
                sentinel_values[val.f64] = np.nan
        except Exception as e:
            logger.warning(
                "%s: null_sentinel attribute's type was no common.capnp:Value.f64! Skipping.",
                Path(__file__).name,
            )


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
            path_to_db_dir = self.config.path_to_db_dir
            temp_dir = None
            try:
                spotpy_params: list[spotpy.parameter.Uniform] = []
                if self.in_ports["init_params"]:
                    try:
                        init_params_ip = await self.read_in("init_params")
                        if init_params_ip is None:
                            self.in_ports["init_params"] = None
                            continue

                        init_params = []
                        if init_params_ip._has("content"):
                            try:
                                init_params = json.loads(params_text := init_params_ip.content.as_text())
                            except Exception as e:
                                logger.warning(
                                    "%s: Couldn't read JSON parameters to calibrate! params: %s",
                                    Path(__file__).name,
                                    params_text,
                                )
                        if len(init_params) == 0:
                            continue

                        for par in init_params:
                            par_name = par["name"]
                            if "array_index" in par:
                                # spotpy does not allow two parameters to have the same name
                                par_name += f"_{par['array_index']}"
                            spotpy_params.append(spotpy.parameter.Uniform(**par))
                        if len(spotpy_params) == 0:
                            logger.warning("%s: no parameters to calibrate!", Path(__file__).name)
                            continue

                    except Exception:
                        logger.exception("%s Exception", Path(__file__).name)
                        continue

                obs_values = None
                param_set_id = self.config.param_set_id
                if self.in_ports["obs_values"]:
                    try:
                        obs_values_ip = await self.read_in("obs_values")
                        if obs_values_ip is None:
                            self.in_ports["obs_values"] = None
                            continue

                        sentinel_values = {}
                        for attr in obs_values_ip.attributes:
                            if attr.key == "param_set_id":
                                param_set_id = attr.value.as_text()
                            check_and_possibly_add_sentinel_value(sentinel_values, attr, "null_sentinel")
                            check_and_possibly_add_sentinel_value(sentinel_values, attr, "nan_sentinel")

                        obs_values = capnp_value_lf64_to_numpy_array_with_nan(
                            obs_values_ip.content.as_struct(common_capnp.Value).lf64, sentinel_values=sentinel_values
                        )
                        if len(obs_values) == 0:
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
                if path_to_db_dir is None:
                    temp_dir = tempfile.TemporaryDirectory()
                    path_to_spotpy_db = f"{temp_dir.name}/SCEUA_results"
                else:
                    path_to_spotpy_db = f"{path_to_db_dir}/SCEUA_results"
                # Set up the sampler with the model above
                if (
                    sampler := instantiate_algorithm(
                        self.config.algorithm, spot_setup, path_to_spotpy_db=path_to_spotpy_db, db_format="csv"
                    )
                ) is None:
                    continue

                # Run the sampler in a thread so the event loop stays free to handle
                # the async port calls made by SpotPySetup.simulation() via
                # asyncio.run_coroutine_threadsafe().
                sample_params = sample_params_for_algorithm(self.config.algorithm, self.config)
                custom_update_sample_params(self.config.algorithm, sample_params, spotpy_params)
                await asyncio.to_thread(sampler.sample, rep, **sample_params)

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

            if temp_dir:
                temp_dir.cleanup()

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
