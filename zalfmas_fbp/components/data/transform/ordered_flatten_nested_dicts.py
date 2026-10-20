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
from typing import Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

JSON_CONTENT_TYPE = "Text (JSON)"


class Config(process.ProcessConfig):
    reverse: bool | list[bool] = Field(
        default=False,
        description=(
            "Sort each nesting level's keys descending instead of ascending. A single value applies "
            "to every level; a list applies its entries level by level, ascending beyond its end."
        ),
    )
    traversal_path: str | None = Field(
        default=None,
        description="Optional path from the document root to the object to flatten.",
    )
    path_separator: str = Field(default="/", description="Separator used for traversal_path.")
    on_error: Literal["skip", "pass_through"] = Field(
        default="skip",
        description="What to do with an IP whose content is not a readable JSON object.",
    )


METADATA = meta.Component(
    category=meta.Category(id="data/transform", name="Data/Transform"),
    info=meta.Info(
        id="c0ec26bf-2d10-4dae-89f6-2e0fea58980e",
        name="ordered flatten nested dicts",
        description=(
            "Flatten a nested JSON object into a list of its leaf values, visiting each level's "
            "keys in sorted order so the result is deterministic. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType=JSON_CONTENT_TYPE, desc="Nested objects to flatten.", required=True),
    ],
    outPorts=[
        meta.Port(name="out", contentType=JSON_CONTENT_TYPE, desc="The leaf values, in order.", required=True),
    ],
    config=Config,
)


def ordered_flatten(nested: Any, reverse: bool | list[bool]) -> list[Any]:
    """Leaf values of a nested mapping, each level's keys visited in sorted order."""
    flattened: list[Any] = []

    def visit(current: Any, reverse_here: bool | list[bool]) -> None:
        if isinstance(reverse_here, list):
            this_level = bool(reverse_here[0]) if reverse_here else False
            deeper: bool | list[bool] = reverse_here[1:] if reverse_here else False
        else:
            this_level = deeper = reverse_here

        if not isinstance(current, dict):
            flattened.append(current)
            return
        for key in sorted(current, reverse=this_level):
            visit(current[key], deeper)

    visit(nested, reverse)
    return flattened


class OrderedFlattenNestedDicts(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        flattened_count = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            try:
                document = json.loads(in_ip.content.as_text())
            except (capnp.KjException, json.JSONDecodeError, UnicodeDecodeError, ValueError):
                logger.warning("%s: content was not readable JSON text.", self.name)
                if self.config.on_error == "pass_through" and not await self.write_out("out", in_ip):
                    break
                continue

            if self.config.traversal_path:
                path = selectors.split_path(self.config.traversal_path, self.config.path_separator)
                document = selectors.apply_path(document, path)
                if document is values.MISSING:
                    logger.warning("%s: traversal_path %r did not resolve.", self.name, self.config.traversal_path)
                    continue

            out_ip = fbp_capnp.IP.new_message(
                content=json.dumps(ordered_flatten(document, self.config.reverse), default=str),
            )
            out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE
            brackets.copy_attrs(in_ip, out_ip)

            flattened_count += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, flattened %d document(s)", self.name, flattened_count)


def main():
    process.run_process_from_metadata_and_cmd_args(OrderedFlattenNestedDicts(METADATA), METADATA)


if __name__ == "__main__":
    main()
