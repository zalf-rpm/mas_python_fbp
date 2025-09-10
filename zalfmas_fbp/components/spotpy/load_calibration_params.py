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

import capnp
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as pp


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await pp.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf"], outs=["params"]
    )
    await pp.update_config_from_port(config, ports["conf"])

    params = []
    if config["path_to_calibrate_csv"]:
        with open(config["path_to_calibrate_csv"]) as params_csv:
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

    if ports["params"]:
        try:
            params_ip = fbp_capnp.IP.new_message(content=json.dumps(params))
            ports["params"].write(value=params_ip).wait()
            await ports["params"].close()

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "path_to_calibrate_csv": "calibratethese.csv",  # path to csv file with calibration parameter information
    "port:conf": "[TOML string] -> component configuration",
    "port:params": "[str]",  # output spotpy calibration params as json list string
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Load the calibration parameters"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
