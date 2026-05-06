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
            "id": "030214e4-7ce8-4de7-8b3c-fb96b7fba7e0",
            "name": "Add content",
            "description": "Add content to incoming IP, optionally moving the old to an attribute."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "in",
                "contentType": "AnyPointer",
                "desc": "Arbitrary IP from upstream."
            }, {
                "name": "content",
                "contentType": "AnyPointer",
                "desc": "Arbitrary content to exchange for 'in's content. Optionally move 'in's content to attribute 'to_attr'"
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "desc": "IP (from in port) with new content from 'content' and possibly old content as attribute 'to_attr'."
            }
        ],
        "defaultConfig": {
            "to_attr": {
                "value": "attr",
                "type": "Text",
                "desc": "The attribute's name to add to the outgoing message."
            }
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in", "content"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    new_content = None
    while ports["in"] and (ports["content"] or new_content) and ports["out"]:
        try:
            if ports["content"]:
                content_msg = await ports["content"].read()
                if content_msg.which() == "done":
                    ports["content"] = None
                    continue
                content_ip = content_msg.value.as_struct(fbp_capnp.IP)
                new_content = content_ip.content

            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue
            in_ip = in_msg.value.as_struct(fbp_capnp.IP)

            out_ip = fbp_capnp.IP.new_message(content=new_content)
            if config["to_attr"]:
                common.copy_and_set_fbp_attrs(in_ip, out_ip, **{config["to_attr"]: in_ip.content})
            else:
                common.copy_and_set_fbp_attrs(in_ip, out_ip)
            await ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
