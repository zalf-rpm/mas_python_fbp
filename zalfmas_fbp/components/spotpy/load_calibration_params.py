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
import os
from typing import Any

from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as pp

meta = {
    "category": {"id": "spotpy", "name": "Spotpy"},
    "component": {
        "info": {
            "id": "028290bb-a38c-4599-9948-fc73723e9654",
            "name": "create SpotPy calibration params",
            "description": "Creates/sets up parameters for Spotpy calibration.",
        },
        "type": "standard",
        "inPorts": [{"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"}],
        "outPorts": [
            {
                "name": "params",
                "contentType": "Text (JSON list)",
                "desc": "output spotpy calibration params as json list string",
            }
        ],
        "defaultConfig": {
            "path_to_calibrate_csv": {
                "value": "calibratethese.csv",
                "type": "string",
                "desc": "path to csv file with parameters to calibrate",
            }
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await pp.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf"], outs=["params"])
    await pp.update_config_from_port(config, pc.in_ports["conf"])

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

    if pc.out_ports["params"]:
        try:
            params_ip = fbp_capnp.IP.new_message(content=json.dumps(params))
            pc.out_ports["params"].write(value=params_ip).wait()
            await pc.out_ports["params"].close()

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
