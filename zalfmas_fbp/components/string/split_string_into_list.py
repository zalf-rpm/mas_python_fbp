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

import logging
import os
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "string", "name": "String"},
    "component": {
        "info": {
            "id": "6c9346e6-71a7-4007-b403-2c78c845c1c7",
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


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

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

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            s = s.rstrip()
            vals = s.split(config["split_at"])
            if cast_to:
                vals = list(map(cast_to, vals))
            # print("split_string vals:", vals)

            req = pc.out_ports["out"].write_request()
            values_list = init_list(req.value.as_struct(fbp_capnp.IP).content, len(vals))
            for i, val in enumerate(vals):
                values_list[i] = val
            await req.send()
            # await pc.out_ports["out"].write(value=vals)

        except capnp.KjException as e:
            logger.error("%s: %s RPC Exception: %s", os.path.basename(__file__), config["name"], e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
