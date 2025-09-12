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
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in", "attr"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    attr = None
    while ports["in"] and (ports["attr"] or attr) and ports["out"]:
        try:
            if ports["attr"]:
                attr_msg = await ports["attr"].read()
                if attr_msg.which() == "done":
                    ports["attr"] = None
                    continue
                attr_ip = attr_msg.value.as_struct(fbp_capnp.IP)
                attr = attr_ip.content

            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue
            in_ip = in_msg.value.as_struct(fbp_capnp.IP)

            out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **{config["to_attr"]: attr})
            await ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": "attr",
    "opt:to_attr": "[string] -> store received content from connected 'attr' port under 'to_attr' name in attributes section of IP",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[anypointer] -> arbitrary content",
    "port:attr": "[anypointer] -> arbitrary content to store as attached attribute with name 'to_attr'",
    "port:out": "[IP + attr] -> IP (from in port) and attribute 'to_attr' containing content from attr port",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Add attribute to IP")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
