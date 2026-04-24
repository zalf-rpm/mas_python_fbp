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

import json
import os

from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "data/transform", "name": "Data/Transform"},
    "component": {
        "info": {
            "id": "c0ec26bf-2d10-4dae-89f6-2e0fea58980e",
            "name": "ordered flatten nested dicts",
            "description": "ordered_flatten_nested_dicts",
        },
        "type": "standard",
        "inPorts": [{"name": "conf"}, {"name": "in"}],
        "outPorts": [{"name": "out"}],
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    reverse = config["reverse"]
    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            in_dict = json.loads(in_ip.content.as_text())

            def ordered_flatten_dict(in_d, out_l, reverse_sort):
                rev, rev_rest = False, False
                if isinstance(reverse_sort, list):
                    if len(reverse_sort) > 0:
                        rev = reverse_sort[0]
                        rev_rest = reverse_sort[1:]
                else:
                    rev, rev_rest = reverse_sort, reverse_sort

                for k in sorted(in_d.keys(), reverse=rev):
                    v = in_d[k]
                    if isinstance(v, dict):
                        ordered_flatten_dict(v, out_l, rev_rest)
                    else:
                        out_l.append(v)

            out_list = []
            ordered_flatten_dict(in_dict, out_list, reverse)

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(out_list))
            await pc.out_ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "reverse": False,  # true or false or [true, true, false] for nesting levels :string of json serialized array
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[]",
    "port:out": "[]",
}


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
