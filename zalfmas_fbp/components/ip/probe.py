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
"""Pass-through logger for looking at a stream without breaking it.

``console/console_output`` is a sink; this forwards what it observes, so a probe can be dropped into
an existing connection. Once the runtime-owned ``log`` port of plan section 6.2 exists, the
observations move onto it as ``LogMessage`` IPs; until then they go to the ordinary logger.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from typing import TYPE_CHECKING, Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

type ShowItem = Literal["count", "type", "content", "attributes", "content_type"]


class Config(process.ProcessConfig):
    label: str = Field("", description="Prefix identifying this probe in the log output.")
    level: Literal["debug", "info", "warning"] = Field("info", description="Level to log observations at.")
    show: list[ShowItem] = Field(
        default_factory=lambda: ["count", "type", "content"],
        description="Which parts of each IP to report, in this order.",
    )
    max_content_chars: int = Field(500, description="Truncate rendered content beyond this length. 0 disables it.")
    every_nth: int = Field(1, description="Report only every nth IP. 1 reports all of them.")
    first_n: int = Field(0, description="Stop reporting after this many IPs. 0 means no limit.")
    as_json: bool = Field(
        False,
        description="Render content as JSON when its Cap'n Proto type can be resolved, rather than as text.",
    )
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none themselves.",
    )
    include_brackets: bool = Field(True, description="Report bracket IPs as well as standard ones.")
    emit_summary_on_close: bool = Field(True, description="Log IP and bracket counts when the input closes.")


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="fc669b60-ae89-46b5-9cee-044f95b01c3b",
        name="Probe",
        description=(
            "Log IPs passing through without altering them. Substream transparent. Leaving 'out' "
            "unconnected turns the probe into a sink."
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
            desc="IPs to observe.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The same IPs, unchanged. May be left unconnected.",
        ),
    ],
    config=Config,
)


class Probe(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.observed: Counter[str] = Counter()

    def _truncate(self, text: str) -> str:
        limit = self.config.max_content_chars
        if limit > 0 and len(text) > limit:
            return f"{text[:limit]}... (+{len(text) - limit} chars)"
        return text

    def _render_content(self, ip: IPReader) -> str:
        resolved = values.python_from_content(ip, self.config.content_type)
        if resolved is values.MISSING:
            # No usable type, and guessing one would misread silently (D14).
            return self._truncate(f"<unreadable content, type {values.content_type_of(ip) or 'unset'}>")
        if self.config.as_json:
            try:
                return self._truncate(json.dumps(resolved, default=str))
            except (TypeError, ValueError):
                pass
        return self._truncate(resolved if isinstance(resolved, str) else str(resolved))

    def _render_attributes(self, ip: IPReader) -> str:
        attrs = brackets.attrs_as_dict(ip)
        if not attrs:
            return "{}"
        rendered = {name: ("<untyped>" if value is values.MISSING else value) for name, value in attrs.items()}
        return self._truncate(json.dumps(rendered, default=str))

    def _describe(self, ip: IPReader, count: int) -> str:
        parts: list[str] = []
        for item in self.config.show:
            match item:
                case "count":
                    parts.append(f"#{count}")
                case "type":
                    parts.append(str(ip.type))
                case "content_type":
                    parts.append(f"contentType={values.content_type_of(ip) or 'unset'}")
                case "attributes":
                    parts.append(f"attrs={self._render_attributes(ip)}")
                case "content":
                    parts.append(self._render_content(ip) if not brackets.is_bracket(ip) else "")
        return " ".join(part for part in parts if part)

    def _should_report(self, count: int) -> bool:
        if self.config.first_n > 0 and count > self.config.first_n:
            return False
        every_nth = max(1, self.config.every_nth)
        return count % every_nth == 0

    def _report(self, message: str) -> None:
        prefix = self.config.label or self.name
        logger.log(logging.getLevelNamesMapping()[self.config.level.upper()], "%s: %s", prefix, message)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        count = 0
        forwarding = self.out_ports["out"] is not None
        if not forwarding:
            logger.info("%s: 'out' is not connected, acting as a sink", self.name)

        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            self.observed[str(in_ip.type)] += 1
            if self.config.include_brackets or not brackets.is_bracket(in_ip):
                count += 1
                if self._should_report(count):
                    self._report(self._describe(in_ip, count))

            if forwarding and not await self.write_out("out", in_ip):
                logger.info("%s: 'out' closed, continuing as a sink", self.name)
                forwarding = False

        if self.config.emit_summary_on_close:
            self._report(f"finished, observed {dict(sorted(self.observed.items()))}")
        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Probe(METADATA), METADATA)


if __name__ == "__main__":
    main()
