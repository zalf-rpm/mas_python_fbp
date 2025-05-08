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
import capnp
import os
import sys
import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.components as c
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

async def run_component(port_infos_reader_sr: str):
    ports = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["in"])

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

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    parser = c.create_default_fbp_component_args_parser("Output text on console FBP component")
    port_infos_reader_sr, _, args = c.handle_default_fpb_component_args(parser)
    asyncio.run(capnp.run(run_component(port_infos_reader_sr)))

if __name__ == '__main__':
    main()
