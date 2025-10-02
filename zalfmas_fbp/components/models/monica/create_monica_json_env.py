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

import capnp
from zalfmas_capnp_schemas import fbp_capnp
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "sim", "crop", "site"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    while ports["sim"] and ports["crop"] and ports["site"] and ports["out"]:
        try:
            sim = await p.read_dict_from_port(ports, "sim")
            crop = await p.read_dict_from_port(ports, "crop")
            site = await p.read_dict_from_port(ports, "site")

            if sim and crop and site:
                env_template = monica_io.create_env_json_from_json_config(
                    {
                        "crop": crop,
                        "site": site,
                        "sim": sim,
                        "climate": "",
                    }
                )

                out_ip = fbp_capnp.IP.new_message()
                to_attr = config.get("to_attr", None)
                if to_attr and len(to_attr.as_text()) > 0:
                    out_ip.attributes = [
                        {"key": to_attr.as_text(), "value": json.dumps(env_template)}
                    ]
                else:
                    out_ip.content = json.dumps(env_template)
                await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": None,
    "port:conf": "[TOML string] -> component configuration",
    "port:sim": "[JSON string] -> sim.json content",
    "port:crop": "[JSON string] -> crop.json content",
    "port:site": "[JSON string] -> site.json content",
    "port:out": "[string (MONICA JSON env)]",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Create a MONICA JSON environment from input sim/crop/site JSON content"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
