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
    "category": {"id": "string", "name": "String"},
    "component": {
        "info": {
            "id": "4b260f85-eb1b-4109-87ec-b30d38a5631a",
            "name": "collect into list",
            "description": "Collects values (parsed out of a string) into a list.",
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "in",
                "contentType": "Text",
                "desc": "Stream of strings, whose values to collect into a single list.",
            },
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "List[Text | Float64 | Int64]"}],
        "defaultConfig": {
            "cast_to": {
                "value": "text",
                "type": ["text", "float", "int"],
                "desc": "Cast text elements to any of these types.",
            }
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, ports["conf"])

    cast_to = None
    if config["cast_to"] == "float":

        def cast_to(v):
            return float(v)
    elif config["cast_to"] == "int":

        def cast_to(v):
            return int(v)

    def init_list(any_p, len_):
        return any_p.init_as_list(capnp._ListSchema(capnp.types.Text), len_)

    if config["cast_to"] == "float":

        def init_list(any_p, len_):
            return any_p.init_as_list(capnp._ListSchema(capnp.types.Float64), len_)
    elif config["cast_to"] == "int":

        def init_list(any_p, len_):
            return any_p.init_as_list(capnp._ListSchema(capnp.types.Int64), len_)

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            s = s.rstrip()
            vals = s.split(config["split_at"])
            if cast_to:
                vals = list(map(cast_to, vals))
            # print("split_string vals:", vals)

            req = ports["out"].write_request()
            l = init_list(req.value.as_struct(fbp_capnp.IP).content, len(vals))
            for i, val in enumerate(vals):
                l[i] = val
            await req.send()
            # await ports["out"].write(value=vals)

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
