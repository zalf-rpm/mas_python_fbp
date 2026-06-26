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

import csv
import json
import logging
from pathlib import Path
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    path_to_calibrate_csv: str = Field(
        "calibratethese.csv",
        description="path to csv file with parameters to calibrate",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="spotpy",
        name="Spotpy",
    ),
    info=meta.Info(
        id="028290bb-a38c-4599-9948-fc73723e9654",
        name="create SpotPy calibration params",
        description="Creates/sets up parameters for Spotpy calibration.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="params",
            contentType="Text (JSON list)",
            desc="output spotpy calibration params as json list string",
        ),
    ],
    config=Config,
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

        params = []
        if self.config.path_to_calibrate_csv:
            try:
                with Path(self.config.path_to_calibrate_csv).open() as params_csv:
                    dialect = csv.Sniffer().sniff(params_csv.read(), delimiters=";,\t")
                    params_csv.seek(0)
                    reader = csv.reader(params_csv, dialect)
                    next(reader, None)  # skip the header
                    for row in reader:
                        p = {"name": row[0]}
                        if len(row[1]) > 0:
                            p["array"] = int(row[1])
                        for n, i in [
                            ("low", 2),
                            ("high", 3),
                            ("step", 4),
                            ("optguess", 5),
                            ("minbound", 6),
                            ("maxbound", 7),
                        ]:
                            if len(row[i]) > 0:
                                p[n] = float(row[i])
                        if len(row) == 9 and len(row[8]) > 0:
                            p["derive_function"] = lambda _, _2: eval(row[8])
                        params.append(p)
            except Exception:
                logger.exception("%s Exception reading CSV", Path(__file__).name)

        if self.out_ports["params"]:
            try:
                params_ip = fbp_capnp.IP.new_message(content=json.dumps(params))
                await self.write_out("params", params_ip)
            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
