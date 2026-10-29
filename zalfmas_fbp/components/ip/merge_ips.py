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
from typing import TYPE_CHECKING, Any, Literal, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    strategy: Literal["next_available", "zip"] = Field(
        "next_available",
        description=(
            "'next_available' interleaves: whichever input has an IP ready goes next, and the "
            "component finishes when every input has closed. 'zip' takes one IP from every input "
            "and combines them, finishing as soon as any input closes."
        ),
    )
    zip_mode: Literal["substream", "attributes", "json_object"] = Field(
        "substream",
        description=(
            "How 'zip' combines one IP per input: wrap them in a substream, fold inputs 1..n into "
            "attributes of input 0, or build a JSON object keyed by input index."
        ),
    )
    zip_attr_prefix: str = Field(
        "in",
        description="Attribute name prefix for the 'attributes' zip mode, giving in1, in2 and so on.",
    )
    tag_source_attr: str | None = Field(
        None,
        description="If set, record which input an IP came from in this attribute, as its 0-based index.",
    )
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none, when building a JSON object.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="ea8826e7-3c60-42f6-bfca-f0a6285a7a63",
        name="Merge IPs",
        description=(
            "Combine several input streams into one - the inverse of 'Copy IP' and 'load "
            "balancer'. Interleaves by availability, or zips one IP from each input together."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            type="array",
            contentType="AnyPointer",
            desc="The streams to merge. Slot order matters for 'zip'.",
            required=True,
        ),
    ],
    outPorts=[meta.Port(name="out", contentType="AnyPointer", desc="The merged stream.", required=True)],
    config=Config,
)


class MergeIPs(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _tagged(self, in_ip: IPReader, index: int) -> IPBuilder | IPReader:
        """A copy of the IP with its source index recorded, or the IP itself if not wanted."""
        if not self.config.tag_source_attr:
            return in_ip
        out_ip = fbp_capnp.IP.new_message(type=in_ip.type)
        out_ip.content = in_ip.content
        if in_ip.sysAttributes.contentType:
            out_ip.sysAttributes.contentType = in_ip.sysAttributes.contentType
        brackets.copy_attrs(in_ip, out_ip, extra={self.config.tag_source_attr: index})
        return out_ip

    async def _run_next_available(self) -> None:
        merged = 0
        while self.out_ports["out"]:
            result = await self.read_array_in_with_index("in")
            if result is None:
                break
            index, in_ip = result
            merged += 1
            if not await self.write_out("out", self._tagged(in_ip, index)):
                break
        logger.info("%s merged %d IP(s)", self.name, merged)

    def _zipped_attributes(self, ips: list[IPReader]) -> IPBuilder:
        """Input 0's content, with the others folded in as attributes."""
        out_ip = fbp_capnp.IP.new_message()
        out_ip.content = ips[0].content
        if ips[0].sysAttributes.contentType:
            out_ip.sysAttributes.contentType = ips[0].sysAttributes.contentType

        extra: dict[str, Any] = {}
        for index, in_ip in enumerate(ips[1:], start=1):
            extra[f"{self.config.zip_attr_prefix}{index}"] = brackets.Attr(
                value=in_ip.content,
                value_type=in_ip.sysAttributes.contentType or None,
            )
        brackets.copy_attrs(ips[0], out_ip, extra=extra)
        return out_ip

    def _zipped_json(self, ips: list[IPReader]) -> IPBuilder | None:
        combined: dict[str, Any] = {}
        for index, in_ip in enumerate(ips):
            resolved = values.python_from_content(in_ip, self.config.content_type)
            if resolved is values.MISSING:
                logger.warning(
                    "%s: input %d carries content with no resolvable type; skipping this group.",
                    self.name,
                    index,
                )
                return None
            combined[str(index)] = resolved

        out_ip = fbp_capnp.IP.new_message(content=json.dumps(combined, default=str))
        out_ip.sysAttributes.contentType = "Text (JSON)"
        brackets.copy_attrs(ips[0], out_ip)
        return out_ip

    async def _emit_zipped(self, ips: list[IPReader]) -> bool:
        if self.config.zip_mode == "substream":
            if not await self.write_out("out", brackets.make_bracket("openBracket")):
                return False
            for index, in_ip in enumerate(ips):
                if not await self.write_out("out", self._tagged(in_ip, index)):
                    return False
            return await self.write_out("out", brackets.make_bracket("closeBracket"))

        combined = self._zipped_attributes(ips) if self.config.zip_mode == "attributes" else self._zipped_json(ips)
        if combined is None:
            return True
        return await self.write_out("out", combined)

    async def _run_zip(self) -> None:
        groups = 0
        while self.out_ports["out"]:
            ips = await self.read_array_in("in", process.ArrayInStrategy.ZIP)
            if not ips:
                break
            groups += 1
            if not await self._emit_zipped(list(ips)):
                break
        logger.info("%s merged %d group(s)", self.name, groups)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if not self.array_in_ports.get("in"):
            logger.warning("%s has no connected inputs; nothing to merge.", self.name)
        elif self.config.strategy == "zip":
            await self._run_zip()
        else:
            await self._run_next_available()

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(MergeIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
