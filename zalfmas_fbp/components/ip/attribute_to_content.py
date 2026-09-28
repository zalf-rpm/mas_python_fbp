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
from __future__ import annotations

import json
import logging
from typing import Any, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

_MISSING = object()  # sub-access couldn't resolve; already logged, skip this IP
_JSON_NATIVE_TYPES = (int, float, bool, type(None), dict, list)


class Config(process.ProcessConfig):
    attr_name: str = Field(
        "attribute_name",
        description="Name of the attribute (on the incoming IP) to extract and promote to the outgoing IP's content.",
    )
    attr_path: str | None = Field(
        None,
        description=(
            "Optional 'attr_sub_access_separator'-delimited path (e.g. 'sub1/sub2') into a sub-object "
            "of the attribute's value. Requires the attribute's capnp type to be described via 'types' "
            "(keyed by '@' + attr_name, same convention as Update JSON) so its value can be cast before "
            "the path is walked. A path segment may itself carry a ':type_ref' suffix (e.g. "
            "'sub1:@othertype'), which looks 'type_ref' up in 'types' and casts to it right after that "
            "segment is applied - needed to keep drilling down whenever a struct field encountered "
            "along the way is itself just an untyped AnyPointer. Leave 'attr_path' unset to just "
            "extract the attribute's value as-is - no type needed in that case."
        ),
    )
    types: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Same convention as Update JSON's 'types' config: maps a type-reference key to the capnp "
            "type it describes (e.g. {'@attribute_name': "
            "'@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup'}). Used to cast the attribute's "
            "value up front (keyed by '@' + attr_name), and again for any ':type_ref' suffix attached "
            "to an 'attr_path' segment. Only needed when 'attr_path' is set."
        ),
    )
    attr_sub_access_separator: str = Field(
        "/",
        description="The token used to separate path segments in 'attr_path', e.g. 'sub1/sub2'.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="8869b77a-4dbf-4fbf-bc95-2a753d443624",
        name="Attribute to content",
        description=(
            "Extract an attribute (optionally a sub-object of it) from an incoming IP and promote it "
            "to be the outgoing IP's content - the inverse of 'add attribute'."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="Input IP whose 'attr_name' attribute (and optionally a sub-object of it) is extracted.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc=(
                "The same IP as received on 'in' (same attributes), but with content replaced by the "
                "extracted attribute value. A plain capnp value (Text, a struct, a list, ...) is used "
                "as-is; a plain Python value that pycapnp's AnyPointer can't hold directly (e.g. a bare "
                "int/float/bool, or anything reached through embedded JSON) is JSON-serialized first."
            ),
        ),
        meta.Port(
            name="pass",
            contentType="AnyPointer",
            desc=(
                "Optional: if connected, every IP received on 'in' - brackets included, and regardless "
                "of whether extraction onto 'out' succeeds - is forwarded here completely unchanged, "
                "so downstream can get both the extracted value and the original IP without needing a "
                "separate copy component."
            ),
        ),
    ],
    config=Config,
)


def _as_type(attr_val: Any, capnp_type_string: str) -> Any:
    schema = common.schema_from_content_type_string(capnp_type_string)
    return common.cast_to_schema(attr_val, schema)


def _split_path(path: str, separator: str) -> list[str | int]:
    # digit-only segments become an int (list/array index) here already; a digit segment that
    # also carries a ':type_ref' suffix (see _sub_access) isn't purely digits, so it survives this
    # pass as a string and is recovered as an int over there instead - same two-step split as
    # Update JSON's split_into_parts()/read_attr_value().
    parts: list[str | int] = []
    for part in path.split(separator):
        if part == "":
            continue
        parts.append(int(part) if part.isdigit() else part)
    return parts


def _sub_access(value: Any, parts: list[str | int], types: dict[str, str], path: str, process_name: str) -> Any:
    """Walk parts (struct field names, or ints for list indices) into value, same core rules as
    Update JSON's read_attr_value:

    - a common.capnp:StructuredText[JSON] value is decoded into its JSON dict/list on the way,
      unless the segment about to be applied literally asks for its raw 'value' field.
    - a segment may carry a ':type_ref' suffix (e.g. 'sub1:@othertype'), which casts the value just
      reached via that type_ref's entry in 'types' before continuing - needed to keep drilling down
      through an AnyPointer field encountered partway through a struct.
    """
    current = value
    for raw_part in parts:
        field_name, _, type_ref = raw_part.partition(":") if isinstance(raw_part, str) else (raw_part, "", "")
        if isinstance(field_name, str) and field_name.isdigit():
            field_name = int(field_name)

        if (
            hasattr(current, "schema")
            and current.schema == common_capnp.StructuredText.schema
            and current.type == "json"
            and field_name != "value"
        ):
            current = json.loads(current.value)

        if isinstance(field_name, int):
            if hasattr(current, "__getitem__") and -len(current) <= field_name < len(current):
                current = current[field_name]
            else:
                logger.error(
                    "%s: index %d out of range while accessing attr_path '%s'; skipping.",
                    process_name,
                    field_name,
                    path,
                )
                return _MISSING
        elif isinstance(current, dict) and field_name in current:
            current = current[field_name]
        elif hasattr(current, "schema") and field_name in current.schema.fieldnames:
            current = getattr(current, field_name)
        else:
            logger.error(
                "%s: couldn't resolve '%s' while accessing attr_path '%s'; skipping.",
                process_name,
                field_name,
                path,
            )
            return _MISSING

        if type_ref:
            capnp_type = types.get(type_ref)
            if capnp_type is None:
                logger.error(
                    "%s: no type for '%s' described in 'types' while accessing attr_path '%s'; skipping.",
                    process_name,
                    type_ref,
                    path,
                )
                return _MISSING
            current = _as_type(current, capnp_type)

    return current


def _prepare_content(value: Any) -> Any:
    """Return something safe to assign as an IP's AnyPointer content: a capnp value (Reader/Builder,
    or a plain str, which pycapnp auto-wraps as Text) is used as-is; any other plain Python value -
    int/float/bool/None/dict/list, which pycapnp's AnyPointer assignment rejects outright - is turned
    into JSON text first.
    """
    if isinstance(value, str) or not isinstance(value, _JSON_NATIVE_TYPES):
        return value
    return json.dumps(value)


class Component(process.Process[Config]):
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
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                continue

            if self.out_ports["pass"]:
                if not await self.write_out("pass", in_ip):
                    self.out_ports["pass"] = None
                    logger.info("%s: error on sending on 'pass' port; continuing without it.", self.name)

            if in_ip.type in ("openBracket", "closeBracket"):
                if not await self.write_out("out", in_ip):
                    logger.info("%s process finished", self.name)
                    return
                continue

            attr = next((kv for kv in in_ip.attributes if kv.key == self.config.attr_name), None)
            if attr is None:
                logger.error(
                    "%s: incoming IP is missing the configured attribute '%s'; skipping.",
                    self.name,
                    self.config.attr_name,
                )
                continue

            value = attr.value
            if self.config.attr_path:
                type_ref = f"@{self.config.attr_name}"
                capnp_type = self.config.types.get(type_ref)
                if capnp_type is None:
                    logger.error(
                        "%s: 'attr_path' is set but no type for '%s' is described in 'types'; skipping.",
                        self.name,
                        type_ref,
                    )
                    continue

                value = _as_type(value, capnp_type)
                parts = _split_path(self.config.attr_path, self.config.attr_sub_access_separator)
                value = _sub_access(value, parts, self.config.types, self.config.attr_path, self.name)
                if value is _MISSING:
                    continue

            out_ip = fbp_capnp.IP.new_message(content=_prepare_content(value), attributes=in_ip.attributes)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
