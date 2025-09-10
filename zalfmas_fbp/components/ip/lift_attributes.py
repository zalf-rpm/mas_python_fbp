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
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    lift_from_type = common.load_capnp_module(config["lift_from_type"])
    lft_fieldnames = lift_from_type.schema.fieldnames
    lift_from_attr_name = config["lift_from_attr"]

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            lift_from_attr = common.get_fbp_attr(
                in_ip, config["lift_from_attr"]
            ).as_struct(lift_from_type)

            out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
            lifted_attrs = config["lifted_attrs"].split(",")
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
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "lift_from_attr": "name",
    "lift_from_type": "schema.capnp:Type",
    "lifted_attrs": "attr1,attr2,attr3",
    "opt:lift_from_attr": "[string (name)] -> attribute to read",
    "opt:lift_from_type": "[struct_capnp:Type] -> capnp struct type to read from attribute",
    "opt:lifted_attrs": "[string (attr1,attr2,attr3,...)] -> which attributes of struct to lift one level up out of struct into metadata",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[]",
    "port:out": "[]",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Lift fields out of in message attributes"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
