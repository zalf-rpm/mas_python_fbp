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
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
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
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
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

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        country_ids = self.config.default_country_ids

        while self.in_ports["country_ids"] and self.out_ports["out"]:
            try:
                country_ids_ip = await self.read_in("country_ids")
                if country_ids_ip is None:
                    self.in_ports["country_ids"] = None
                    continue

                c_ids_txt = country_ids_ip.content.as_text()
                if len(c_ids_txt):
                    country_ids = json.loads(c_ids_txt)
                    if isinstance(country_ids, int):
                        country_ids = [country_ids]

                crop_to_country_to_year_to_value = defaultdict(lambda: defaultdict(dict))
                with Path(self.config.path_to_yield_data).open() as file:
                    dialect = csv.Sniffer().sniff(file.read(), delimiters=";,\t")
                    file.seek(0)
                    reader = csv.reader(file, dialect)
                    next(reader, None)  # skip the header
                    for row in reader:
                        crop = row[0].strip().lower()
                        country_id = int(row[4])
                        year = int(row[2])
                        value = float(row[3]) * 1000.0  # t/ha -> kg/ha
                        if country_ids is None or len(country_ids) == 0 or country_id in country_ids:
                            crop_to_country_to_year_to_value[crop][country_id][year] = value

                # fill in no data values
                for _crop, country_to_year_to_value in crop_to_country_to_year_to_value.items():
                    for _country_id, year_to_value in country_to_year_to_value.items():
                        for year in range(self.config.from_year, self.config.to_year + 1):
                            if year not in year_to_value:
                                year_to_value[year] = self.config.no_data_value

                param_set_id = "-".join([str(id) for id in country_ids])
                out_ip = fbp_capnp.IP.new_message(
                    attributes=[{"key": "param_set_id", "value": param_set_id}],
                    content=json.dumps(crop_to_country_to_year_to_value.get(self.config.crop, {})),
                )
                await self.write_out("out", out_ip)

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
