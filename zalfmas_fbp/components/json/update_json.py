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

from zalfmas_capnp_schemas_with_stubs import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "json",
        "name": "JSON"
    },
    "component": {
        "info": {
            "id": "67b31990-452a-4058-8a99-6785be345216",
            "name": "Update JSON",
            "description": "Update JSON datastructures."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "in",
                "contentType": "Text (JSON)",
                "desc": "JSON text which is supposed to be updated and changed."
            }
        ],
        "outPorts": [
            {
                "name": "out",
                "contentType": "Text (JSON)",
                "desc": "The updated JSON text."
            }
        ],
        "defaultConfig": {
            "types": {
                "value": {
                    "@setup": "mas.schema.model.monica.sim_setup_capnp:Setup"
                },
                "type": "object",
                "desc": "Define the loadable type the attribute being referenced has."
            },
            "update": {
                "value": [
                    ["climate", "csv-options", "start-date", "<-", ["@setup", "startDate"]],
                    ["climate", "csv-options", "end-date", "<-", "1999-01-09"],
                    ["customId_ex", "<-", 1],
                    ["cropRotationTemplate_ex", "WW", 0, "worksteps", 0, "date", "<-", "2020-01-03"]
                ],
                "type": "array",
                "desc": "List of update operations to perform on the JSON structure. Structure and description have to match."
            },
            "replace": {
                "value": [],
                "type": "array",
                "desc": "List of replacement operations to perform on the JSON structure."
            },
            "add": {
                "value": [],
                "type": "array",
                "desc": "List of addition operations to perform on the JSON structure."
            }
        }
    }
}


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
            def change(j, spec: dict, allowed_operation="update"):
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

            def create_nested_dict(l: list) -> dict:
                res_dict = {}
                d = res_dict
                prev_d = d
                prev_key = None
                assign_result = False
                for val in l:
                    if assign_result:
                        prev_d[prev_key] = val
                        continue
                    if val == "<-":
                        assign_result = True
                        continue
                    d[str(val)] = {}
                    prev_d = d
                    prev_key = str(val)
                    d = d[str(val)]
                return res_dict

            for op in ["update", "replace", "add"]:
                if op in config:
                    for co in config[op]:
                        try:
                            nested_dict = create_nested_dict(co)
                            change(j_content, nested_dict, allowed_operation=op)
                        except:
                            pass

            out_ip = fbp_capnp.IP.new_message(
                content=json.dumps(j_content),
                attributes=list([{"key": k, "value": v} for k, v in attrs.items()]),
            )
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
