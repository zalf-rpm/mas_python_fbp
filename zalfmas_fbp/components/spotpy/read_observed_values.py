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
import csv
import json
import os
from collections import defaultdict

import capnp
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "country_ids"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    # get default country ids
    country_ids = config["default_country_ids"]

    while ports["country_ids"] and ports["out"]:
        try:
            msg = await ports["country_ids"].read()
            if msg.which() == "done":
                ports["country_ids"] = None
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
                    if (
                        country_ids is None
                        or len(country_ids) == 0
                        or country_id in country_ids
                    ):
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
                content=json.dumps(
                    crop_to_country_to_year_to_value.get(config["crop"], {})
                ),
            )

            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "path_to_yield_data": "data/FAO_yield_data.csv",
    "crop": "maize",  # maize | millet | sorghum
    "from_year": 2010,
    "to_year": 2020,
    "no_data_value": -9999,
    "default_country_ids": [],
    "port:conf": "[TOML string] -> component configuration",
    "port:country_ids": None,  # [1,2,3] :string of serialized json array containing country ids
    "port:out": "",  # {country_id: {year: yield}} :string of json serialized mapping from country id to year to yield
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Copy IP to all attached array out ports"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
