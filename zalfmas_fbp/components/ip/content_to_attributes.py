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

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    paths: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Attribute name -> selector, e.g. {'site': './site/id'}. Usually content paths, but any selector works."
        ),
    )
    keep_content: bool = Field(default=True, description="Leave the content in place as well.")
    as_type: Literal["value", "json", "text"] = Field(
        default="value",
        description=(
            "How to store each extracted value: a common.capnp:Value, JSON text, or plain text. "
            "'value' is the library convention and keeps the value typed."
        ),
    )
    on_missing: Literal["skip", "null", "fail"] = Field(
        default="skip",
        description="What an unresolvable selector does: leave the attribute out, store null, or drop the IP.",
    )
    path_separator: str = Field(default="/", description="Separator used inside selectors.")
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
        id="ae18965d-c034-4836-ae44-5becd7f602a0",
        name="Content to attributes",
        description=(
            "Lift parts of an IP's content into attributes, so later components can route, group "
            "or name by them without re-reading the content. The inverse of 'Attribute to content'."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to lift from.", required=True)],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="The IPs with the new attributes.", required=True),
    ],
    config=Config,
)


class ContentToAttributes(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _stored(self, value: Any) -> Any:
        if self.config.as_type == "json":
            return json.dumps(value, default=str)
        if self.config.as_type == "text":
            return value if isinstance(value, str) else json.dumps(value, default=str)
        return value

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if not self.config.paths:
            logger.warning("%s has no 'paths' configured; IPs pass through unchanged.", self.name)

        lifted = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            extra: dict[str, Any] = {}
            dropped = False
            for name, selector in self.config.paths.items():
                resolved = selectors.resolve(
                    in_ip,
                    selector,
                    separator=self.config.path_separator,
                    content_type=self.config.content_type,
                    attr_types=self.config.attr_types,
                )
                if resolved is values.MISSING:
                    if self.config.on_missing == "fail":
                        logger.warning("%s: %r did not resolve; dropping this IP.", self.name, selector)
                        dropped = True
                        break
                    if self.config.on_missing == "null":
                        extra[name] = ""
                    continue
                extra[name] = self._stored(resolved)

            if dropped:
                continue

            out_ip = fbp_capnp.IP.new_message()
            if self.config.keep_content:
                out_ip.content = in_ip.content
                if in_ip.sysAttributes.contentType:
                    out_ip.sysAttributes.contentType = in_ip.sysAttributes.contentType
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            lifted += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, lifted from %d IP(s)", self.name, lifted)


def main():
    process.run_process_from_metadata_and_cmd_args(ContentToAttributes(METADATA), METADATA)


if __name__ == "__main__":
    main()
