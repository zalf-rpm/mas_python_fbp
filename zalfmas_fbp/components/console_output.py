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
from zalfmas_fbp.run.ports import connect_ports_from_port_infos_reader as connect_ports
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

async def main(port_infos_reader_sr: str = None):
    port_infos_reader_sr = port_infos_reader_sr or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not port_infos_reader_sr:
        print("Usage: console_output.py <port_infos_reader_sr>")
        sys.exit(1)

    ins, outs, close_out_ports, con_man = await connect_ports(port_infos_reader_sr, ins=["in"])

    try:
        while ins["in"]:
            in_msg = await ins["in"].read()
            if in_msg.which() == "done":
                ins["in"] = None
                continue
            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            print(in_ip.content.as_text())
    except Exception as e:
        ins["in"] = None
        print(f"{os.path.basename(__file__)} Exception:", e)

    await close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))
