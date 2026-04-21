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

from zalfmas_capnp_schemas_with_stubs import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "ip",
        "name": "IP (Flow packages)"
    },
    "component": {
        "info": {
            "id": "b1e875af-4ee7-4937-8824-17d185216ec4",
            "name": "copy",
            "description": "Copy IP to multiple outputs."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "in",
                "contentType": "AnyPointer",
                "desc": "The IP to copy to all attached outports"
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "type": "array",
                "contentType": "AnyPointer",
                "desc": "Copied IP for each attached outport"
            }
        ]
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    while ports["in"] and any(ports["out"]):
        try:
            msg = await ports["in"].read()
            # check for end of data from in port
            if msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = msg.value.as_struct(fbp_capnp.IP)
            for out_p in ports["out"]:
                if out_p:
                    out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
                    common.copy_and_set_fbp_attrs(in_ip, out_ip)
                    await out_p.write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
