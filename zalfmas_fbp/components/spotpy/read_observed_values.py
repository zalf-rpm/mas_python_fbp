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
import os
from collections import defaultdict

from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "spotpy", "name": "Spotpy"},
    "component": {
        "info": {
            "id": "993e5cdf-1c55-4a75-9538-e7906676fedb",
            "name": "read observed values",
            "description": "Read the observed values for the calibration.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "country_ids",
                "contentType": "Text (JSON Array or Number)",
                "desc": "[1,2,3] :string of serialized json array containing country ids",
            },
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "Text",
                "desc": "{country_id: {year: yield}} :string of json serialized mapping from country id to year to yield",
            }
        ],
        "defaultConfig": {
            "path_to_yield_data": {"value": "data/FAO_yield_data.csv", "type": "string", "desc": "path to yield data"},
            "crop": {"value": "maize", "type": "string", "desc": "crop to calibrate, e.g. maize | millet | sorghum"},
            "from_year": {"value": 2010, "type": "int", "desc": "start year for calibration"},
            "to_year": {"value": 2020, "type": "int", "desc": "end year for calibration"},
            "no_data_value": {"value": -9999, "type": "int", "desc": "no data value"},
            "default_country_ids": {
                "value": [],
                "type": "list",
                "desc": "string of serialized json array containing country ids",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "country_ids"], outs=["out"]
    )
    await p.update_config_from_port(config, pc.in_ports["conf"])

    # get default country ids
    country_ids = config["default_country_ids"]

    while pc.in_ports["country_ids"] and pc.out_ports["out"]:
        try:
            msg = await pc.in_ports["country_ids"].read()
            if msg.which() == "done":
                pc.in_ports["country_ids"] = None
                continue
            country_ids_ip = msg.value.as_struct(fbp_capnp.IP)
            c_ids_txt = country_ids_ip.content.as_text()
            if len(c_ids_txt):
                country_ids = json.loads(c_ids_txt)
                if isinstance(country_ids, int):
                    country_ids = [country_ids]

            crop_to_country_to_year_to_value = defaultdict(lambda: defaultdict(dict))
            with open(config["path_to_yield_data"]) as file:
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
            from_year = config["from_year"]
            to_year = config["to_year"]
            for (
                crop,
                country_to_year_to_value,
            ) in crop_to_country_to_year_to_value.items():
                for country_id, year_to_value in country_to_year_to_value.items():
                    for year in range(from_year, to_year + 1):
                        if year not in year_to_value:
                            year_to_value[year] = config["no_data_value"]

            param_set_id = "-".join([str(id) for id in country_ids])
            out_ip = fbp_capnp.IP.new_message(
                attributes=[{"key": "param_set_id", "value": param_set_id}],
                content=json.dumps(crop_to_country_to_year_to_value.get(config["crop"], {})),
            )

            await pc.out_ports["out"].write(value=out_ip)

        except Exception:
            logger.exception("%s Exception", os.path.basename(__file__))

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
