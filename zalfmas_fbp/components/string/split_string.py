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
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "string", "name": "String"},
    "component": {
        "info": {
            "id": "d5c2fc62-2be0-4a25-aafe-e710ac3fb39c",
            "name": "split string",
            "description": "Splits a string along delimiter.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "in", "contentType": "Text"},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "Text"}],
        "defaultConfig": {"split_at": {"value": ",", "type": "string", "desc": "Split string at this character."}},
    },
}


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    print(f"{os.path.basename(__file__)}: {config['name']} connected port(s)")
    _ = await p.update_config_from_port(config, pc.in_ports["conf"])
    if pc.in_ports["conf"]:
        print(f"{os.path.basename(__file__)}: {config['name']} updated config from config port")

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            print(f"{os.path.basename(__file__)}: {config['name']} received:", s)
            s = s.rstrip()
            vals = s.split(config["split_at"])

            for val in vals:
                out_ip = fbp_capnp.IP.new_message(content=val)
                await pc.out_ports["out"].write(value=out_ip)
                print(f"{os.path.basename(__file__)}: {config['name']} sent:", val)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: {config['name']} process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
