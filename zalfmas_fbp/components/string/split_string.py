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

import os

import capnp
from zalfmas_capnp_schemas_with_stubs import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "string",
        "name": "String"
    },
    "component": {
        "info": {
            "id": "d5c2fc62-2be0-4a25-aafe-e710ac3fb39c",
            "name": "split string",
            "description": "Splits a string along delimiter."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "in",
                "contentType": "Text"
            }, {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "Text"
            }
        ],
        "defaultConfig": {
            "split_at": {
                "value": ",",
                "type": "string",
                "desc": "split string at this character"
            }
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    print(f"{os.path.basename(__file__)}: {config['name']} connected port(s)")
    await p.update_config_from_port(config, ports["conf"])
    if ports["conf"]:
        print(
            f"{os.path.basename(__file__)}: {config['name']} updated config from config port"
        )

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            print(f"{os.path.basename(__file__)}: {config['name']} received:", s)
            s = s.rstrip()
            vals = s.split(config["split_at"])

            for val in vals:
                out_ip = fbp_capnp.IP.new_message(content=val)
                await ports["out"].write(value=out_ip)
                print(f"{os.path.basename(__file__)}: {config['name']} sent:", val)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: {config['name']} process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
