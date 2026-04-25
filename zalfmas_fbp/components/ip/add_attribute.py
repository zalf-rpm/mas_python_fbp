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

import capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "ip", "name": "IP (Flow packages)"},
    "component": {
        "info": {
            "id": "1d442f41-dee4-4973-ad99-09855af1d7ad",
            "name": "add attribute",
            "description": "Add attribute to incoming IP.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {"name": "in", "contentType": "AnyPointer", "desc": "Arbitrary content."},
            {
                "name": "attr",
                "contentType": "AnyPointer",
                "desc": "Arbitrary content to store as attached attribute with name 'to_attr'.",
            },
        ],
        "outPorts": [
            {"name": "out", "desc": "IP (from in port) and attribute 'to_attr' containing content from attr port."}
        ],
        "defaultConfig": {
            "to_attr": {"value": "attr", "type": "Text", "desc": "The attribute's name to add to the outgoing message."}
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in", "attr"], outs=["out"]
    )
    await p.update_config_from_port(config, pc.in_ports["conf"])

    attr = None
    while pc.in_ports["in"] and (pc.in_ports["attr"] or attr) and pc.out_ports["out"]:
        try:
            if pc.in_ports["attr"]:
                attr_msg = await pc.in_ports["attr"].read()
                if attr_msg.which() == "done":
                    pc.in_ports["attr"] = None
                    continue
                attr_ip = attr_msg.value.as_struct(fbp_capnp.IP)
                attr = attr_ip.content

            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue
            in_ip = in_msg.value.as_struct(fbp_capnp.IP)

            out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **{config["to_attr"]: attr})
            await pc.out_ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            logger.error("%s: %s RPC Exception: %s", os.path.basename(__file__), config["name"], e.description)

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
