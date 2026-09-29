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

import logging
from typing import Literal, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, templating
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    pattern: str = Field(
        "{.}",
        description=(
            "Text with {...} placeholders: '{@attr}' or '{@attr/sub}' for an attribute, '{.}' or "
            "'{./a/b}' for the content, '{#type}' for IP metadata, '{count}' for the IP's position "
            "in the stream, and '{now:%Y-%m-%d}' for the time. A format spec after ':' works as in "
            "str.format, e.g. '{count:03d}'. Use '{{' and '}}' for literal braces."
        ),
    )
    to_attr: str | None = Field(
        None,
        description="Write the result to this attribute instead of replacing the content.",
    )
    missing: Literal["error", "empty", "keep"] = Field(
        "empty",
        description=(
            "What an unresolvable placeholder does: skip the IP with a warning, render as empty, "
            "or leave the placeholder in the output as written."
        ),
    )
    number_format: str | None = Field(
        None,
        description="Format spec applied to numbers that have no explicit one, e.g. '.2f'.",
    )
    path_separator: str = Field("/", description="Separator used inside selectors.")
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none, when a placeholder reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType, by attribute name.",
    )


METADATA = meta.Component(
    category=meta.Category(id="string", name="String"),
    info=meta.Info(
        id="7b8828a2-936e-4301-a907-0d6c2c73a558",
        name="Format string",
        description=(
            "Build a string from an IP's attributes, content and position, into the content or an "
            "attribute. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to render from.", required=True)],
    outPorts=[
        meta.Port(
            name="out", contentType="Text", desc="The rendered string, or the IP with it attached.", required=True
        ),
    ],
    config=Config,
)


class FormatString(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        try:
            templating.validate_pattern(self.config.pattern)
        except templating.TemplateError as exc:
            logger.error("%s: %s", self.name, exc)  # noqa: TRY400 - the message is the whole point
            return

        count = 0
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
                rendered = templating.render(
                    self.config.pattern,
                    in_ip,
                    count=count,
                    separator=self.config.path_separator,
                    content_type=self.config.content_type,
                    attr_types=self.config.attr_types,
                    missing=self.config.missing,
                    number_format=self.config.number_format,
                )
            except templating.TemplateError as exc:
                logger.warning("%s: %s; skipping this IP.", self.name, exc)
                count += 1
                continue

            out_ip = fbp_capnp.IP.new_message()
            if self.config.to_attr:
                out_ip.content = in_ip.content
                if in_ip.sysAttributes.contentType:
                    out_ip.sysAttributes.contentType = in_ip.sysAttributes.contentType
                brackets.copy_attrs(in_ip, out_ip, extra={self.config.to_attr: rendered})
            else:
                out_ip.content = rendered
                out_ip.sysAttributes.contentType = "Text"
                brackets.copy_attrs(in_ip, out_ip)

            count += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, rendered %d IP(s)", self.name, count)


def main():
    process.run_process_from_metadata_and_cmd_args(FormatString(METADATA), METADATA)


if __name__ == "__main__":
    main()
