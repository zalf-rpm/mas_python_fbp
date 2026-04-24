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
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "console", "name": "Console"},
    "component": {
        "info": {
            "id": "2de9c491-d8a6-4b36-84de-db7f4a312731",
            "name": "output to console",
            "description": "Output input to console.",
        },
        "type": "standard",
        "inPorts": [{"name": "in"}],
        "outPorts": [],
    },
}


async def run_component(port_infos_reader_sr: str, config: dict[str, str]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["in"])
    print(f"{os.path.basename(__file__)}: connected port(s)")

    while pc.in_ports["in"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            try:
                print(in_ip.content.as_text())
            except Exception:
                print(in_ip.content)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
