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
import sys
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


class Config(process.ProcessConfig):
    show: list[Literal["content", "attributes", "type", "content_type", "count"]] = Field(
        default_factory=lambda: ["content"],
        description="Which parts of each IP to print, in this order.",
    )
    show_brackets: bool = Field(
        default=False,
        description="Print bracket IPs too, so a substream's shape is visible.",
    )
    as_json: bool = Field(
        default=False,
        description="Render content as JSON when its Cap'n Proto type can be resolved.",
    )
    content_type: str | None = Field(
        default=None,
        description="Content type to assume for IPs that carry none.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType; '*' covers all.",
    )
    separator: str = Field(default=" ", description="Joiner between the printed parts of one IP.")
    max_chars: int = Field(default=0, description="Truncate each printed IP beyond this length. 0 disables it.")


METADATA = meta.Component(
    category=meta.Category(id="console", name="Console"),
    info=meta.Info(
        id="2de9c491-d8a6-4b36-84de-db7f4a312731",
        name="output to console",
        description=(
            "Print incoming IPs to stdout. A sink: it has no output, so use 'Probe' to watch a "
            "stream without breaking it. Bracket IPs are counted but only printed on request."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to print.", required=True)],
    outPorts=[],
    config=Config,
)


class ConsoleOutput(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.printed: int = 0

    def _content_of(self, in_ip: IPReader) -> str:
        resolved = values.python_from_content(in_ip, self.config.content_type)
        if resolved is values.MISSING:
            # No usable type, and guessing one would misread silently (D14).
            return f"<unreadable content, type {values.content_type_of(in_ip) or 'unset'}>"
        if self.config.as_json:
            try:
                return json.dumps(resolved, default=str)
            except (TypeError, ValueError):
                pass
        return resolved if isinstance(resolved, str) else str(resolved)

    def line_for(self, in_ip: IPReader, count: int) -> str:
        parts: list[str] = []
        for item in self.config.show:
            match item:
                case "count":
                    parts.append(f"#{count}")
                case "type":
                    parts.append(str(in_ip.type))
                case "content_type":
                    parts.append(f"contentType={values.content_type_of(in_ip) or 'unset'}")
                case "attributes":
                    attrs = brackets.attrs_as_dict(in_ip, self.config.attr_types)
                    rendered = {k: ("<untyped>" if v is values.MISSING else v) for k, v in attrs.items()}
                    parts.append(json.dumps(rendered, default=str))
                case "content":
                    parts.append("" if brackets.is_bracket(in_ip) else self._content_of(in_ip))

        line = self.config.separator.join(part for part in parts if part)
        if self.config.max_chars > 0 and len(line) > self.config.max_chars:
            line = f"{line[: self.config.max_chars]}... (+{len(line) - self.config.max_chars} chars)"
        return line

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        count = 0
        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip) and not self.config.show_brackets:
                continue

            count += 1
            sys.stdout.write(f"{self.line_for(in_ip, count)}\n")
            sys.stdout.flush()
            self.printed += 1

        logger.info("%s process finished, printed %d IP(s)", self.name, self.printed)


def main():
    process.run_process_from_metadata_and_cmd_args(ConsoleOutput(METADATA), METADATA)


if __name__ == "__main__":
    main()
