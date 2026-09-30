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

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

JSON_CONTENT_TYPE = "Text (JSON)"


class Config(process.ProcessConfig):
    direction: Literal["to_content", "to_attributes"] = Field(
        default="to_content",
        description=(
            "'to_content' gathers attributes into a JSON object as the content. 'to_attributes' "
            "does the reverse, exploding a JSON object's keys into attributes."
        ),
    )
    only: list[str] = Field(
        default_factory=list,
        description="Restrict to these attribute names (or object keys). Empty means all of them.",
    )
    exclude: list[str] = Field(default_factory=list, description="Attribute names (or keys) to leave out.")
    keep_attributes: bool = Field(
        default=False,
        description="For 'to_content': also keep the attributes on the outgoing IP.",
    )
    keep_content: bool = Field(
        default=False,
        description="For 'to_attributes': keep the JSON object as the content as well.",
    )
    include_content_under: str | None = Field(
        default=None,
        description="For 'to_content': also place the original content under this key.",
    )
    content_type: str | None = Field(
        default=None,
        description="Content type to assume for IPs that carry none.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType; '*' covers all.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="cf980fe1-c991-4e41-9ae8-55b81ea20f92",
        name="Attributes to content",
        description=(
            "Gather an IP's attributes into a JSON object as its content, or explode a JSON object "
            "into attributes. Where 'Attribute to content' promotes one attribute, this moves the set."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to convert.", required=True)],
    outPorts=[meta.Port(name="out", contentType="AnyPointer", desc="The converted IPs.", required=True)],
    config=Config,
)


class AttributesToContent(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _selected(self, names: list[str]) -> list[str]:
        only, exclude = set(self.config.only), set(self.config.exclude)
        return [name for name in names if (not only or name in only) and name not in exclude]

    def _to_content(self, in_ip: Any) -> Any:
        attrs = brackets.attrs_as_dict(in_ip, self.config.attr_types)
        gathered = {
            name: (None if attrs[name] is values.MISSING else attrs[name]) for name in self._selected(list(attrs))
        }
        if self.config.include_content_under:
            content = values.python_from_content(in_ip, self.config.content_type)
            gathered[self.config.include_content_under] = None if content is values.MISSING else content

        out_ip = fbp_capnp.IP.new_message(content=json.dumps(gathered, default=str))
        out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE
        if self.config.keep_attributes:
            brackets.copy_attrs(in_ip, out_ip)
        return out_ip

    def _to_attributes(self, in_ip: Any) -> Any | None:
        content = values.python_from_content(in_ip, self.config.content_type)
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except (json.JSONDecodeError, ValueError):
                pass
        if not isinstance(content, dict):
            logger.warning("%s: content is not a JSON object; forwarding the IP unchanged.", self.name)
            return None

        extra = {name: content[name] for name in self._selected(list(content))}
        out_ip = fbp_capnp.IP.new_message()
        if self.config.keep_content:
            out_ip.content = in_ip.content
            if in_ip.sysAttributes.contentType:
                out_ip.sysAttributes.contentType = in_ip.sysAttributes.contentType
        brackets.copy_attrs(in_ip, out_ip, extra=extra)
        return out_ip

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        converted = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            if self.config.direction == "to_content":
                out_ip = self._to_content(in_ip)
            else:
                out_ip = self._to_attributes(in_ip)
                if out_ip is None:
                    if not await self.write_out("out", in_ip):
                        break
                    continue

            converted += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, converted %d IP(s)", self.name, converted)


def main():
    process.run_process_from_metadata_and_cmd_args(AttributesToContent(METADATA), METADATA)


if __name__ == "__main__":
    main()
