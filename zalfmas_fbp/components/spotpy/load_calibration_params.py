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
from typing import Any, Literal, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


NUMERIC_COLUMNS = (("low", 2), ("high", 3), ("step", 4), ("optguess", 5), ("minbound", 6), ("maxbound", 7))


class Config(process.ProcessConfig):
    path_to_calibrate_csv: str = Field(
        "calibratethese.csv",
        description="path to csv file with parameters to calibrate",
    )
    delimiter: str = Field(
        "",
        description="Column separator. Empty works it out from the file, falling back to a comma.",
    )
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether an unreadable row is skipped or stops the process.",
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
    inPorts=[],
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

        params = self.read_params()
        if self.out_ports["params"]:
            params_ip = fbp_capnp.IP.new_message(content=json.dumps(params))
            if not await self.write_out("params", params_ip):
                logger.warning("%s: could not send the parameters.", self.name)

        logger.info("%s process finished, loaded %d parameter(s)", self.name, len(params))

    def read_params(self) -> list[dict[str, Any]]:
        """The calibration parameters from the CSV, one dict per row."""

        if not self.config.path_to_calibrate_csv:
            return []
        path = Path(self.config.path_to_calibrate_csv)
        try:
            text = path.read_text()
        except OSError:
            if self.config.on_error == "fail":
                raise
            logger.exception("%s: could not read %s", self.name, path)
            return []

        params: list[dict[str, Any]] = []
        reader = csv.reader(text.splitlines(), delimiter=self.delimiter_for(text))
        next(reader, None)  # skip the header
        for line_no, row in enumerate(reader, start=2):
            param = self.param_from_row(row, path, line_no)
            if param is not None:
                params.append(param)
        return params

    def delimiter_for(self, text: str) -> str:
        """The configured separator, or one worked out from the file.

        `csv.Sniffer` raises on plenty of perfectly good files - a short one, or one whose rows
        end in empty fields - and the component used to give up entirely and load nothing when it
        did. A comma is a far better answer than no parameters at all.
        """

        if self.config.delimiter:
            return self.config.delimiter
        try:
            return csv.Sniffer().sniff(text, delimiters=";,\t").delimiter
        except csv.Error:
            logger.info("%s: could not work out the delimiter; assuming a comma.", self.name)
            return ","

    def param_from_row(self, row: list[str], path: Path, line_no: int) -> dict[str, Any] | None:
        """One parameter, or None if the row cannot be read as one."""

        try:
            param: dict[str, Any] = {"name": row[0]}
            if len(row[1]) > 0:
                param["array_index"] = int(row[1])
            for name, index in NUMERIC_COLUMNS:
                if len(row[index]) > 0:
                    param[name] = float(row[index])
            if len(row) > 8 and len(row[8]) > 0:
                # Carried as text: this travels to the sampler as JSON, and a function cannot.
                # It used to be built here as `lambda _, _2: eval(row[8])`, which both closed over
                # the loop variable - so every row ended up with the *last* row's expression - and
                # made the whole list unserialisable, so the component silently sent nothing at all.
                param["derive_expression"] = row[8]
        except (IndexError, ValueError):
            message = f"{self.name}: {path}:{line_no} is not a readable parameter row"
            if self.config.on_error == "fail":
                raise ValueError(message) from None
            logger.warning(message)
            return None
        return param


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
