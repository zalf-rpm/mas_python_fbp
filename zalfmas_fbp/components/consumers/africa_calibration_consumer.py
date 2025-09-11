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
import json
import os
from collections import defaultdict
from datetime import datetime

import capnp
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "result"], outs=["year_to_yield"]
    )
    await p.update_config_from_port(config, ports["conf"])

    path_to_out_file = os.path.join(config["path_to_out"], "/consumer.out")
    if not os.path.exists(config["path_to_out"]):
        try:
            os.makedirs(config["path_to_out"])
        except OSError:
            print(
                "run-calibration-consumer.py: Couldn't create dir:",
                config["path_to_out"],
                "!",
            )
    with open(path_to_out_file, "a") as _:
        _.write(f"config: {config}\n")

    country_id_to_year_to_yields = defaultdict(lambda: defaultdict(list))

    envs_received = 0
    no_of_envs_expected = None
    close_out_port = False
    while ports["result"] and ports["year_to_yield"]:
        try:
            msg = await ports["result"].read()
            if msg.which() == "done":
                close_out_port = True
                continue

            result_ip = msg.value.as_struct(fbp_capnp.IP)
            monica_result = json.loads(result_ip.content.as_text())

            custom_id = monica_result["customId"]
            if "no_of_sent_envs" in custom_id:
                no_of_envs_expected = custom_id["no_of_sent_envs"]
            else:
                envs_received += 1

                out_str = f"{os.path.basename(__file__)}: received result customId: {custom_id}\n"
                print(out_str)
                with open(path_to_out_file, "a") as _:
                    _.write(out_str)

                country_id = custom_id["country_id"]

                for data in monica_result.get("data", []):
                    results = data.get("results", [])
                    for vals in results:
                        if "Year" in vals:
                            country_id_to_year_to_yields[country_id][
                                int(vals["Year"])
                            ].append(vals["Yield"])

            if no_of_envs_expected == envs_received:
                out_str = f"{os.path.basename(__file__)}: {datetime.now()} last expected env received\n"
                print(out_str)
                with open(path_to_out_file, "a") as _:
                    _.write(out_str)

                country_id_and_year_to_avg_yield = {}
                for country_id, rest in country_id_to_year_to_yields.items():
                    for year, yields in rest.items():
                        no_of_yields = len(yields)
                        if no_of_yields > 0:
                            country_id_and_year_to_avg_yield[f"{country_id}|{year}"] = (
                                sum(yields) / no_of_yields
                            )

                out_ip = fbp_capnp.IP.new_message(
                    content=json.dumps(country_id_and_year_to_avg_yield)
                )
                await ports["year_to_yield"].write(value=out_ip)

                # reset and wait for next round
                country_id_to_year_to_yields.clear()
                no_of_envs_expected = None
                envs_received = 0

                if close_out_port:
                    ports["result"] = None

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "path_to_out": "out/",
    "port:conf": "[TOML string] -> component configuration",
    "port:result": None,  # json object string (json serialized MONICA result)
    "port:year_to_yield": None,  # json object string (json serialized mapping of year to yields)
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
