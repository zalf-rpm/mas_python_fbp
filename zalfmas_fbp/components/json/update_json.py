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
import logging
from pathlib import Path
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class CompConfig(process.ProcessConfig):
    types: dict[str, str] = Field(
        {"@setup": "mas.schema.model.monica.sim_setup_capnp:Setup"},
        description="Define the loadable type the attribute being referenced has.",
    )
    update: list[list[str | int | list[str | int]]] = Field(
        [
            ["climate", "csv-options", "start-date", "<-", ["@setup", "startDate"]],
            ["climate", "csv-options", "end-date", "<-", "1999-01-09"],
            ["customId_ex", "<-", 1],
            ["cropRotationTemplate_ex", "WW", 0, "worksteps", 0, "date", "<-", "2020-01-03"],
        ],
        description="List of update operations to perform on the JSON structure. Structure and description have to match.",
    )
    replace: list[list[str | int | list[str | int]]] = Field(
        [],
        description="List of replacement operations to perform on the JSON structure.",
    )
    add: list[list[str | int | list[str | int]]] = Field(
        [],
        description="List of addition operations to perform on the JSON structure.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="67b31990-452a-4058-8a99-6785be345216",
        name="Update JSON",
        description="Update JSON datastructures.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="JSON text which is supposed to be updated and changed.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="The updated JSON text.",
        ),
    ],
    config=CompConfig,
)


class Component(process.Process[CompConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info(f"{self.name} process running")
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            try:
                if not (in_ip := await self.read_in("in")):
                    self.in_ports["in"] = None
                    continue

                attrs = {kv.key: kv.value for kv in in_ip.attributes}
                j_content = json.loads(in_ip.content.as_text())

                def as_type(attr_val, capnp_type_desc: str):
                    if capnp_type_desc.lower() == "text":
                        return attr_val.as_text()
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
                            # j[k] is a pointer, so we can recurse
                            elif type(j[k]) in [list, dict]:
                                change(j[k], v, allowed_operation)
                            elif allowed_operation == "replace":
                                j[k] = v
                        # a list as value is treated as sub object access if the first element is an attribute (@) access
                        elif type(v) is list:
                            # attribute access
                            if len(v) >= 1 and type(v[0]) is str and len(v[0]) > 0 and v[0][0] == "@":
                                attr_val, is_capnp = p.get_attr_val(
                                    v[0],
                                    attrs,
                                    remove=False,
                                )
                                # attribute sub access
                                if is_capnp and len(v) > 1 and v[0] in self.config.types:
                                    attr_val = as_type(attr_val, self.config.types[v[0]])
                                    is_json = False
                                    for field_name in v[1:]:
                                        attr_dir = attr_val.__dir__()
                                        # check if this is common.capnp/StructuredText
                                        if (
                                            "schema" in attr_dir
                                            and attr_val.schema.node.id == 17108059578820121684
                                            and attr_val.type == "json"
                                        ):
                                            is_json = True
                                            attr_val = json.loads(attr_val.value)

                                        # is array index
                                        if isinstance(field_name, int):
                                            if len(attr_val) > field_name:
                                                attr_val = attr_val[field_name]
                                        # is json access
                                        elif is_json and field_name in attr_val:
                                            attr_val = attr_val[field_name]
                                        # is struct access
                                        elif "schema" in attr_dir and field_name in attr_val.schema.fieldnames:
                                            attr_val = attr_val.__getattribute__(field_name)
                                v = attr_val
                            # access a list
                            if i and type(j) is list:
                                j[i] = v
                            # access a dict
                            else:
                                j[k] = v
                        elif type(v) is str:
                            # use existing function to resolve values from attributes
                            attr_val, is_attr_val = p.get_attr_val(
                                spec[k],
                                attrs,
                                remove=False,
                            )
                            if is_attr_val and spec[k] in self.config.types:
                                attr_val = as_type(attr_val, self.config.types[spec[k]])
                            if i and type(j) is list:
                                j[i] = attr_val
                            else:
                                j[k] = attr_val
                        elif i and type(j) is list:
                            j[i] = v
                        else:
                            j[k] = v

                def create_nested_dict(path_parts: list) -> dict:
                    res_dict = {}
                    d = res_dict
                    prev_d = d
                    prev_key = None
                    assign_result = False
                    for val in path_parts:
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
                    for co in self.config.__getattribute__(op):
                        try:
                            nested_dict = create_nested_dict(co)
                            change(j_content, nested_dict, allowed_operation=op)
                        except (AttributeError, IndexError, KeyError, TypeError, ValueError) as e:
                            logger.warning(
                                "%s: couldn't apply %s operation %s: %s",
                                Path(__file__).name,
                                op,
                                co,
                                e,
                            )

                out_ip = fbp_capnp.IP.new_message(
                    content=json.dumps(j_content),
                    attributes=list([{"key": k, "value": v} for k, v in attrs.items()]),  # pyright: ignore
                )
                await self.write_out("out", out_ip)

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s: process finished", Path(__file__).name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
