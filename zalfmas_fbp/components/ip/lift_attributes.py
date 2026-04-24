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

from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "ip", "name": "IP (Flow packages)"},
    "categoryId": "ip",
    "component": {
        "info": {
            "id": "1ccc2798-23b2-4148-a40f-6b70a69be2fb",
            "name": "lift attributes",
            "description": "Lift attributes.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {
                "name": "in",
                "contentType": "AnyPointer",
                "desc": "Input message with any content containing some structured attributes.",
            },
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "AnyPointer",
                "desc": "The same content as the message on 'in', but with some attributes lifted out of a structured attribute into the top level attribute metadata.",
            }
        ],
        "defaultConfig": {
            "lift_from_attr": {"value": "name", "type": "string", "desc": "Attribute to read from IP."},
            "lift_from_type": {
                "value": "schema.capnp:Type",
                "type": "string",
                "desc": "Capnp struct type to read from attribute.",
            },
            "lifted_attrs": {
                "value": ["attr1", "attr2", "attr3"],
                "type": "List",
                "desc": "Attributes to lift from struct into metadata.",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    lift_from_type = common.load_capnp_module(config["lift_from_type"])
    lft_fieldnames = lift_from_type.schema.fieldnames

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            lift_from_attr = common.get_fbp_attr(in_ip, config["lift_from_attr"]).as_struct(lift_from_type)

            out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
            lifted_attrs = config["lifted_attrs"]
            attrs = []
            for attr in in_ip.attributes:
                attrs.append(attr)
            if lift_from_attr:
                for l_attr_name in lifted_attrs:
                    if l_attr_name in lft_fieldnames:
                        attrs.append(
                            {
                                "key": l_attr_name,
                                "value": lift_from_attr.__getattr__(l_attr_name),
                            }
                        )
            out_ip.attributes = attrs
            await pc.out_ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
