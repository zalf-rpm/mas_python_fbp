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
from typing import Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


def as_type(
    attr_val, capnp_type_string: str
) -> tuple[
    Any,
    capnp.lib.capnp._StructSchema
    | capnp.lib.capnp._InterfaceSchema
    | capnp.lib.capnp._EnumSchema
    | capnp.lib.capnp._SchemaType,
]:
    schema = common.schema_from_content_type_string(capnp_type_string)
    return common.cast_to_schema(attr_val, schema), schema


def read_attr_value(
    types: dict[str, str], attrs: dict[str, capnp.lib.capnp._DynamicObjectReader], v: list
) -> tuple[Any, bool]:
    # attribute access, where the attribute referenced via @ has to be found in the IPs attributes
    # and this attribute necessarily has to be a capnp struct
    if len(v) >= 1 and type(v[0]) is str and len(v[0]) > 0 and v[0][0] == "@":
        attr_val, is_capnp = p.get_attr_val(
            v[0],
            attrs,
            remove=False,
        )
        # attribute sub access
        if is_capnp and len(v) > 1 and v[0] in types:
            attr_val, _ = as_type(attr_val, types[v[0]])
            is_json = False
            sub_access_len = len(v[1:])
            for i, field_name in enumerate(v[1:]):
                attr_dir = attr_val.__dir__()
                # check if this is common.capnp/StructuredText[JSON], in that case get values
                # out of a JSON dict, but only if the user didn't want to access the value directly
                # (next subaccess would have been value)
                if (
                    "schema" in attr_dir
                    and attr_val.schema == common_capnp.StructuredText.schema  # 17108059578820121684
                    and attr_val.type == "json"
                    and ((sub_access_len > (1 + i) and v[1 + i + 1] != "value") or sub_access_len > i)
                ):
                    is_json = True
                    attr_val = json.loads(attr_val.value)

                # is array index
                if isinstance(field_name, int):
                    if hasattr(attr_val, "__getitem__") and len(attr_val) > field_name:
                        attr_val = attr_val[field_name]
                # is json access
                elif is_json and field_name in attr_val:
                    attr_val = attr_val[field_name]
                # is struct access
                elif "schema" in attr_dir and field_name in attr_val.schema.fieldnames:
                    attr_val = attr_val.__getattribute__(field_name)
        return attr_val, True

    return None, False


def read_dict_value(py_dict: dict[str, Any], v: list) -> tuple[Any, bool]:
    if len(v) >= 1 and type(v[0]) is str and len(v[0]) > 0 and v[0] in py_dict:
        attr_val = py_dict[v[0]]
        for field_name in v[1:]:
            if isinstance(field_name, int):
                if hasattr(attr_val, "__getitem__") and len(attr_val) > field_name:
                    attr_val = attr_val[field_name]
            elif field_name in attr_val:
                attr_val = attr_val[field_name]
        return attr_val, True
    return None, False


def split_into_parts(str_value: str, split_token: str = "/", create_int_indizes=False):
    parts = str_value.split(split_token)
    if create_int_indizes:
        for k in range(len(parts)):
            if parts[k].isdigit():
                parts[k] = int(parts[k])
    return parts


class CompConfig(process.ProcessConfig):
    types: dict[str, str] = Field(
        {"@setup": "@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup"},
        description="Define the loadable type the attribute being referenced has.",
    )
    attr_sub_access_separator: str = Field(
        "/",
        description="The token to be used to separate path names for accessing sub-objects, e.g. @attr_object/sub1/sub2.",
    )
    update: dict[str, str | int | float | bool | None] = Field(
        {
            "climate/csv-options/start-date": "@setup/startDate",
            "climate/csv-options/end-date": "1999-01-09",
            "customId_ex": 1,
            "cropRotationTemplate_ex/WW/0/worksteps/0/date": "2020-01-03",
        },
        description="Mapping of update operations to perform on the JSON structure. Structure and description have to match.",
    )
    replace: dict[str, str | int | float | bool | None] = Field(
        {},
        description="Mapping of replacement operations to perform on the JSON structure.",
    )
    add: dict[str, str | int | float | bool | None] = Field(
        {},
        description="Mapping of addition operations to perform on the JSON structure.",
    )
    name_for_attrs_port: str = Field(
        "attrs",
        description="""The content of a message received on the 'attrs' port will be made available under that name for the updates as an attribute.
        By default this will be the ports name, thus '@attrs'.""",
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
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="JSON text which is supposed to be updated and changed.",
        ),
        meta.Port(
            name="attrs",
            contentType="AnyPointer",
            desc="""If connected, then for each message received on 'attrs', the attributes and the content of that message will be made available
            to the component as '@attrs' for the content of the message and each attribute as '@attrs_{attr_name}', if any. By default the used name will be
            the ports name ('attrs'), unless it has been renamed via the config option 'name_for_attrs_port'.""",
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
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            try:
                if not (in_ip := await self.read_in("in")):
                    self.in_ports["in"] = None
                    continue

                attrs = {kv.key: kv.value for kv in in_ip.attributes}
                j_content = json.loads(in_ip.content.as_text())

                if self.in_ports["attrs"]:
                    if attrs_ip := await self.read_in("attrs"):
                        if attrs_ip._has("content"):
                            attrs[self.config.name_for_attrs_port] = attrs_ip.content
                        for kv in attrs_ip.attributes:
                            attrs[f"{self.config.name_for_attrs_port}_{kv.key}"] = kv.value
                    else:
                        self.in_ports["attrs"] = None
                        continue

                # allowed_operation = update | replace | add
                # update = structures of j and spec have to match exactly
                # replace = if j doesn't match, sub-parts will be replaced according to spec
                # add = if keys are missing, the will be added and set to according sub-spec
                def change(json_obj, spec: dict, allowed_operation="update"):
                    for spec_key, spec_value in spec.items():
                        spec_index = int(spec_key) if spec_key.isdigit() else None
                        # check the key against j to be changed
                        # the key is an index and points outside the array j
                        if spec_index is not None and type(json_obj) is list:
                            if spec_index >= len(json_obj):
                                if allowed_operation == "add":
                                    json_obj.extend([None] * (spec_index + 1 - len(json_obj)))
                                    json_obj[spec_index] = spec_value
                                else:
                                    continue
                        # the key is not in the dict j
                        elif type(json_obj) is dict:
                            if spec_key not in json_obj:
                                if allowed_operation == "add":
                                    json_obj[spec_key] = spec_value
                                else:
                                    continue

                        # change according to the type of v
                        # v is a sub-spec (dict), which means recurse into substructure
                        if type(spec_value) is dict:
                            # access a list
                            if spec_index is not None and type(json_obj) is list:
                                # j[i] is a pointer, so we recurse
                                if type(json_obj[spec_index]) in [list, dict]:
                                    change(json_obj[spec_index], spec_value, allowed_operation)
                                elif allowed_operation == "replace":
                                    json_obj[spec_index] = spec_value
                            # access a dict
                            # j[k] is a pointer, so we can recurse
                            elif type(json_obj[spec_key]) in [list, dict]:
                                change(json_obj[spec_key], spec_value, allowed_operation)
                            elif allowed_operation == "replace":
                                json_obj[spec_key] = spec_value
                        # a list as value is treated as sub object access if the first element is an attribute (@) access
                        elif type(spec_value) is list:
                            attr_val, read_successful = read_attr_value(self.config.types, attrs, spec_value)
                            if read_successful:
                                spec_value = attr_val
                            # access a list
                            if spec_index is not None and type(json_obj) is list:
                                json_obj[spec_index] = spec_value
                            # access a dict
                            else:
                                json_obj[spec_key] = spec_value
                        elif type(spec_value) is str:
                            # use existing function to resolve values from attributes
                            attr_val, is_attr_val = p.get_attr_val(
                                spec[spec_key],
                                attrs,
                                remove=False,
                            )
                            if is_attr_val and spec[spec_key] in self.config.types:
                                attr_val, _ = as_type(attr_val, self.config.types[spec[spec_key]])
                            if spec_index is not None and type(json_obj) is list:
                                json_obj[spec_index] = attr_val
                            else:
                                json_obj[spec_key] = attr_val
                        elif spec_index is not None and type(json_obj) is list:
                            json_obj[spec_index] = spec_value
                        else:
                            json_obj[spec_key] = spec_value

                def create_nested_dict(kvs: dict[str, str]) -> dict:
                    res_dict = {}
                    for key, val in kvs.items():
                        d = res_dict
                        key_parts = split_into_parts(key, self.config.attr_sub_access_separator)
                        for i, key_part in enumerate(key_parts):
                            # unless it's the last part, created nested dicts
                            if i < len(key_parts) - 1:
                                if key_part not in d:
                                    d[key_part] = {}
                                d = d[key_part]
                            else:
                                # if the value for the last part is an actual attribute (with potential sub-access)
                                if isinstance(val, str) and len(val) > 0 and val[0] == "@":
                                    val_parts = split_into_parts(
                                        val, self.config.attr_sub_access_separator, create_int_indizes=True
                                    )
                                    d[key_part] = val_parts
                                else:
                                    # just assign the value to the correct sub dict entry
                                    d[key_part] = val
                    return res_dict

                for op in ["update", "replace", "add"]:
                    changes = self.config.__getattribute__(op)
                    try:
                        nested_dict = create_nested_dict(changes)
                        change(j_content, nested_dict, allowed_operation=op)
                    except (AttributeError, IndexError, KeyError, TypeError, ValueError) as e:
                        logger.warning(
                            "%s: couldn't apply %s operation %s: %s",
                            Path(__file__).name,
                            op,
                            changes,
                            e,
                        )

                out_ip = fbp_capnp.IP.new_message(
                    content=json.dumps(j_content),
                    attributes=in_ip.attributes,  # list([{"key": k, "value": v} for k, v in attrs.items()]),  # pyright: ignore
                )
                await self.write_out("out", out_ip)

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s: process finished", Path(__file__).name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
