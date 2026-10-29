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
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    path_to_yield_data: str = Field(
        "data/FAO_yield_data.csv",
        description="path to yield data",
    )
    crop: str = Field(
        "maize",
        description="crop to calibrate, e.g. maize | millet | sorghum",
    )
    from_year: int = Field(
        2010,
        description="start year for calibration",
    )
    to_year: int = Field(
        2020,
        description="end year for calibration",
    )
    no_data_value: int = Field(
        -9999,
        description="no data value",
    )
    default_country_ids: list[int] = Field(
        default_factory=list,
        description="string of serialized json array containing country ids",
    )
    delimiter: str = Field(
        "",
        description="Column separator of the yield CSV. Empty works it out, falling back to a comma.",
    )
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether an unreadable input or yield row is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="spotpy",
        name="Spotpy",
    ),
    info=meta.Info(
        id="993e5cdf-1c55-4a75-9538-e7906676fedb",
        name="read observed values",
        description="Read the observed values for the calibration.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="country_ids",
            contentType="Text (JSON Array or Number)",
            desc="[1,2,3] :string of serialized json array containing country ids",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="{country_id: {year: yield}} :string of json serialized mapping from country id to year to yield",
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
        self._yields: dict[str, dict[int, dict[int, float]]] = {}
        self._yields_from: str | None = None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        country_ids = self.config.default_country_ids

        sent = 0
        while self.in_ports["country_ids"] and self.out_ports["out"]:
            in_ip = await self.read_in("country_ids")
            if in_ip is None:
                self.in_ports["country_ids"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            wanted = self.country_ids_of(in_ip, country_ids)
            if wanted is None:
                message = f"{self.name}: no country ids could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue
            country_ids = wanted

            observed = self.observed_for(country_ids)
            out_ip = fbp_capnp.IP.new_message(
                attributes=[{"key": "param_set_id", "value": "-".join(str(c) for c in country_ids)}],
                content=json.dumps(observed),
            )
            sent += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, sent %d set(s) of observed values", self.name, sent)

    def country_ids_of(self, in_ip: Any, current: list[int]) -> list[int] | None:
        """The country ids this IP asks for, or the ones in force if it names none."""

        try:
            text = in_ip.content.as_text()
        except capnp.KjException:
            return None
        if not text:
            return current
        try:
            ids = json.loads(text)
        except (TypeError, ValueError):
            return None
        if isinstance(ids, int):
            return [ids]
        if isinstance(ids, list) and all(isinstance(c, int) for c in ids):
            return ids
        return None

    def yield_data(self) -> dict[str, dict[int, dict[int, float]]]:
        """Every yield row, by crop, country and year. Read once: the file does not change."""

        path = Path(self.config.path_to_yield_data)
        if self._yields_from == str(path):
            return self._yields

        try:
            text = path.read_text()
        except OSError:
            if self.config.on_error == "fail":
                raise
            logger.exception("%s: could not read %s", self.name, path)
            return {}

        by_crop: dict[str, dict[int, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))
        reader = csv.reader(text.splitlines(), delimiter=self.delimiter_for(text))
        next(reader, None)  # skip the header
        for line_no, row in enumerate(reader, start=2):
            try:
                crop = row[0].strip().lower()
                by_crop[crop][int(row[4])][int(row[2])] = float(row[3]) * 1000.0  # t/ha -> kg/ha
            except (IndexError, ValueError):
                # one malformed row used to abandon the whole file for that IP
                message = f"{self.name}: {path}:{line_no} is not a readable yield row"
                if self.config.on_error == "fail":
                    raise ValueError(message) from None
                logger.warning(message)

        self._yields = {crop: dict(countries) for crop, countries in by_crop.items()}
        self._yields_from = str(path)
        return self._yields

    def delimiter_for(self, text: str) -> str:
        """The configured separator, or one worked out from the file, falling back to a comma.

        `csv.Sniffer` raises on plenty of good files, and giving up meant reading no data at all.
        """

        if self.config.delimiter:
            return self.config.delimiter
        try:
            return csv.Sniffer().sniff(text, delimiters=";,\t").delimiter
        except csv.Error:
            logger.info("%s: could not work out the delimiter; assuming a comma.", self.name)
            return ","

    def observed_for(self, country_ids: list[int]) -> dict[int, dict[int, float]]:
        """The configured crop's values for these countries, with missing years filled in."""

        wanted = self.yield_data().get(self.config.crop, {})
        observed: dict[int, dict[int, float]] = {}
        for country_id, year_to_value in wanted.items():
            if country_ids and country_id not in country_ids:
                continue
            filled = dict(year_to_value)
            for year in range(self.config.from_year, self.config.to_year + 1):
                filled.setdefault(year, self.config.no_data_value)
            observed[country_id] = filled
        return observed


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
