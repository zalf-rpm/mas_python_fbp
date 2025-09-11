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
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["in"]
    )
    print(f"{os.path.basename(__file__)}: connected port(s)")

    while ports["in"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
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

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Output text on console FBP component"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(parser)
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
