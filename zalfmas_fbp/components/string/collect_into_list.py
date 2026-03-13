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
            "id": "4b260f85-eb1b-4109-87ec-b30d38a5631a",
            "name": "collect into list",
            "description": "Collects values (parsed out of a string) into a list."
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
                "contentType": "List[Text | Float64 | Int64]"
            }
        ],
        "defaultConfig": {
            "no_of_elements": {
                "value": "all",
                "type": ["string", "int"],
                "desc": "number of elements to collect into array"
            },
            "cast_to": {
                "value": "text",
                "type": ["text", "float", "int"],
                "desc": "cast text elements to these types"
            }
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    no_of_elements = 0 if "no_of_elements" == "all" else int(config["no_of_elements"])

    cast_to = None
    if config["cast_to"] == "float":
        cast_to = lambda v: float(v)
    elif config["cast_to"] == "int":
        cast_to = lambda v: int(v)

    init_list = lambda any_p, len_: any_p.init_as_list(
        capnp._ListSchema(capnp.types.Text), len_
    )
    if config["cast_to"] == "float":
        init_list = lambda any_p, len_: any_p.init_as_list(
            capnp._ListSchema(capnp.types.Float64), len_
        )
    elif config["cast_to"] == "int":
        init_list = lambda any_p, len_: any_p.init_as_list(
            capnp._ListSchema(capnp.types.Int64), len_
        )

    while ports["in"] and ports["out"]:
        try:
            elems = []
            while no_of_elements == 0 or len(elems) < no_of_elements:
                in_msg = await ports["in"].read()
                if in_msg.which() == "done":
                    ports["in"] = None
                    break

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                elems.append(s)

            if cast_to:
                elems = list(map(cast_to, elems))
            print(f"{os.path.basename(__file__)}:", elems)

            req = ports["out"].write_request()
            l = init_list(req.value.as_struct(fbp_capnp.IP).content, len(elems))
            for i, val in enumerate(elems):
                l[i] = val
            await req.send()

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
