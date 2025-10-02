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
import json
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

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            # check for end of data from in port
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attrs = {kv.key: kv.value for kv in in_ip.attributes}
            j_content = json.loads(in_ip.content.as_text())

            def as_type(attr_val, capnp_type_desc: str):
                if capnp_type_desc.lower() == "text":
                    return attr_val.as_text()
                else:
                    struct_type, _ = common.load_capnp_module(capnp_type_desc)
                    return attr_val.as_struct(struct_type)

            # allowed_operation = update | replace | add
            # update = structures of j and spec have to match exactly
            # replace = if j doesn't match, sub-parts will be replaced according to spec
            # add = if keys are missing, the will be added and set to according sub-spec
            def change(j, spec, allowed_operation="update"):
                for k, v in spec.items():
                    i = int(k) if k.isdigit() else None
                    # check the key against j to be changed
                    # the key is an index and points outside the array j
                    if i and type(j) is list:
                        if i >= len(j):
                            if allowed_operation == "add":
                                j.extend([None] * (i + 1 - len(j)))
                                j[i] = v
                            else:
                                continue
                    # the key is not in the dict j
                    elif type(j) is dict:
                        if k not in j:
                            if allowed_operation == "add":
                                j[k] = v
                            else:
                                continue

                    # change according to the type of v
                    # v is a sub-spec (dict), which means recurse into substructure
                    if type(v) is dict:
                        # access a list
                        if i and type(j) is list:
                            # j[i] is a pointer, so we recurse
                            if type(j[i]) in [list, dict]:
                                change(j[i], v, allowed_operation)
                            elif allowed_operation == "replace":
                                j[i] = v
                        # access a dict
                        else:
                            # j[k] is a pointer, so we can recurse
                            if type(j[k]) in [list, dict]:
                                change(j[k], v, allowed_operation)
                            elif allowed_operation == "replace":
                                j[k] = v
                    # a list as value is treated as sub object access if the first element is an attribute (@) access
                    elif type(v) is list:
                        # attribute access
                        if (
                            len(v) >= 1
                            and type(v[0]) is str
                            and len(v[0]) > 0
                            and v[0][0] == "@"
                        ):
                            attr_val, is_capnp = p.get_attr_val(
                                v[0],
                                attrs,
                                remove=False,
                            )
                            # attribute sub access
                            if (
                                is_capnp
                                and len(v) > 1
                                and "types" in config
                                and v[0] in config["types"]
                            ):
                                attr_val = as_type(attr_val, config["types"][v[0]])
                                for field_name in v[1:]:
                                    if attr_val._has(field_name):
                                        attr_val = attr_val._get(field_name)
                            v = attr_val
                        # access a list
                        if i and type(j) is list:
                            j[i] = v
                        # access a dict
                        else:
                            j[k] = v
                    elif type(v) is str:
                        # use existing function to resolve values from attributes
                        attr_val, is_capnp = p.get_attr_val(
                            spec[k],
                            attrs,
                            remove=False,
                        )
                        if (
                            is_capnp
                            and "types" in config
                            and spec[k] in config["types"]
                        ):
                            attr_val = as_type(attr_val, config["types"][spec[k]])
                        if i and type(j) is list:
                            j[i] = attr_val
                        else:
                            j[k] = attr_val
                    else:
                        if i and type(j) is list:
                            j[i] = v
                        else:
                            j[k] = v

            for op in ["update", "replace", "add"]:
                if op in config:
                    change(j_content, config[op], allowed_operation=op)

            out_ip = fbp_capnp.IP.new_message(
                content=json.dumps(j_content),
                attributes=list([{"key": k, "value": v} for k, v in attrs.items()]),
            )
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    # "update": {
    #     "customId": {"id": 1, "bla": 2},
    #     "customId2": 5,
    #     "params": {"siteParameters": {"Latitude": 100}},
    #     "cropRotationTemplates.WW.0.worksteps = 5": None,
    #     "cropRotationTemplates": {"WW": {"0": {"worksteps": 5}}},
    #     "cropRotationTemplates.WW.0.1.worksteps = 5": None,
    #     "cropRotationTemplates.WW.0.1.worksteps = '@some_attr'": None,
    #     "cropRotationTemplates": {"WW": {"0": {"1": {"worksteps": 5}}}},
    # },
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[JSON string]",
    "port:out": "[JSON string] -> updated JSON string",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Update JSON datastructure.")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
