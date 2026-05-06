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
            "id": "4b260f85-eb1b-4109-87ec-b30d38a5631a",
            "name": "Collect into list",
            "description": "Collects values (parsed out of a string) into a list.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "in", "contentType": "Text"},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "List[Text | Float64 | Int64]"}],
        "defaultConfig": {
            "no_of_elements": {
                "value": 0,
                "type": "int",
                "desc": "Number of elements to collect into array. no_of_elements <= 0 means collect all elements from upstream. This means the component waits for the 'in' port to close, before sending a message on out.",
            },
            "cast_to": {
                "value": "text",
                "type": ["text", "float", "int"],
                "desc": "Cast received value on 'in' port to either text, float or int values.",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    no_of_elements = config["no_of_elements"]

    def cast_value(value: str):
        if config["cast_to"] == "float":
            return float(value)
        if config["cast_to"] == "int":
            return int(value)
        return value

    def init_list(any_pointer: capnp.lib.capnp._DynamicObjectBuilder, length: int):
        list_type = capnp.types.Text
        if config["cast_to"] == "float":
            list_type = capnp.types.Float64
        elif config["cast_to"] == "int":
            list_type = capnp.types.Int64
        return any_pointer.init_as_list(capnp._ListSchema(list_type), length)

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            elems = []
            while no_of_elements <= 0 or len(elems) < no_of_elements:
                in_msg = await pc.in_ports["in"].read()
                if in_msg.which() == "done":
                    pc.in_ports["in"] = None
                    break

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                elems.append(s)

            if config["cast_to"] != "text":
                elems = [cast_value(elem) for elem in elems]
            logger.info("%s: %s", os.path.basename(__file__), elems)

            req = pc.out_ports["out"].write_request()
            values_list = init_list(req.value.as_struct(fbp_capnp.IP).content, len(elems))
            for i, val in enumerate(elems):
                values_list[i] = val
            await req.send()

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
