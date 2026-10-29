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

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

_PARSE_FAILED = object()  # content wasn't usable JSON, skip this IP


class Config(process.ProcessConfig):
    flatten: bool = Field(
        False,
        description=(
            "If true, flatten the concatenated list by 'flatten_levels' levels before it is written "
            "on 'out': any list-valued element is spliced into the result in place of itself, rather "
            "than kept nested. If false (the default), the output is simply one list entry per IP "
            "received in the substream, in order, whatever shape each entry's own JSON value has."
        ),
    )
    flatten_levels: int = Field(
        1,
        description=(
            "How many levels deep to flatten the concatenated list when 'flatten' is true. Ignored "
            "otherwise. 1 unwraps each top-level list-valued entry once; higher values keep unwrapping "
            "further nested lists inside those."
        ),
    )
    remove_attrs: list[str] = Field(
        default_factory=list,
        description=(
            "Names of attributes to drop when merging attributes across the substream's IPs. "
            "Attributes are otherwise merged in incoming IP order - a later IP's attribute value "
            "overwrites an earlier one of the same name - and written onto the single output IP."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="70b6eafd-2e22-4c3b-a886-93d3a72664e1",
        name="Concat JSON substream",
        description=(
            "Concatenate a substream of JSON IPs received on 'in' into one JSON list written on "
            "'out', optionally flattening nested lists and merging attributes across the substream."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc=(
                "A substream (open-bracket ... close-bracket) of JSON IPs to concatenate into one "
                "JSON list. Every IP is expected to arrive inside a substream; one arriving outside "
                "of one is skipped with a warning, since concatenating a substream is this "
                "component's whole purpose. Multiple consecutive substreams are each collected and "
                "emitted independently, until 'in' closes."
            ),
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc=(
                "One JSON list per completed substream received on 'in': the concatenation (optionally "
                "flattened) of that substream's parsed JSON values, carrying the merged and "
                "'remove_attrs'-filtered attributes from all of that substream's IPs."
            ),
        ),
    ],
    config=Config,
)


def _parse_json(content: str, process_name: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        logger.warning("%s received invalid JSON on 'in': %s", process_name, exc)
        return _PARSE_FAILED


def _flatten(items: list[Any], levels: int) -> list[Any]:
    if levels <= 0:
        return items
    flattened: list[Any] = []
    for item in items:
        if isinstance(item, list):
            flattened.extend(_flatten(item, levels - 1))
        else:
            flattened.append(item)
    return flattened


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    async def _collect_substream(self) -> tuple[list[Any], dict[str, Any]]:
        """Read (blocking) until the matching close-bracket, collecting each inner IP's parsed JSON
        value and merging its (remove_attrs-filtered) attributes. Assumes the opening open-bracket
        on 'in' has already been consumed by the caller.
        """
        remove_attrs = set(self.config.remove_attrs)
        items: list[Any] = []
        merged_attrs: dict[str, Any] = {}
        nesting_level = 1

        while nesting_level > 0:
            ip = await self.read_in("in")
            if ip is None:
                logger.warning(
                    "%s: 'in' port closed mid-substream; emitting whatever was collected so far.",
                    self.name,
                )
                self.in_ports["in"] = None
                break
            if ip.type == "openBracket":
                nesting_level += 1
                continue
            if ip.type == "closeBracket":
                nesting_level -= 1
                continue

            parsed = _parse_json(ip.content.as_text(), self.name)
            if parsed is _PARSE_FAILED:
                continue
            items.append(parsed)
            for attr in ip.attributes:
                if attr.key not in remove_attrs:
                    merged_attrs[attr.key] = attr

        return items, merged_attrs

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                continue

            if in_ip.type != "openBracket":
                logger.warning(
                    "%s: IP of type '%s' received on 'in' outside of a substream; ignoring.",
                    self.name,
                    in_ip.type,
                )
                continue

            items, merged_attrs = await self._collect_substream()

            if self.config.flatten:
                items = _flatten(items, self.config.flatten_levels)

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(items))
            attrs = out_ip.init("attributes", len(merged_attrs))
            for i, attr in enumerate(merged_attrs.values()):
                attrs[i].key = attr.key
                if attr._has("desc"):  # noqa: SLF001 - only set if actually present, see split_bracketed_stream.py
                    attrs[i].desc = attr.desc
                attrs[i].value = attr.value
                if attr._has("valueType"):  # noqa: SLF001
                    attrs[i].valueType = attr.valueType

            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
