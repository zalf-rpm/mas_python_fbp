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


class Config(process.ProcessConfig):
    trigger_on: Literal["stream_end", "substream_end", "both"] = Field(
        default="stream_end",
        description=(
            "When to emit: once when the input closes, once per substream as its close-bracket "
            "passes, or both."
        ),
    )
    content: str = Field(default="done", description="Content of the emitted IP.")
    as_type: Literal["text", "value", "json"] = Field(
        default="text",
        description="How to encode the content: plain text, a common.capnp:Value, or JSON text.",
    )
    count_attr: str | None = Field(
        default="ip_count",
        description="If set, record how many IPs went past on the emitted IP as this attribute.",
    )
    forward_input: bool = Field(
        default=True,
        description="Forward the incoming IPs as well. Off makes this a sink that only signals.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="e9fde9d5-249c-4329-b651-6ef82b79a176",
        name="On stream end",
        description=(
            "Emit an IP once the input has finished, so later work can be sequenced after it. The "
            "'now that everything is written, do X' primitive."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="The stream to watch.", required=True)],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The input stream, if forwarded. Optional.",
        ),
        meta.Port(
            name="end",
            contentType="AnyPointer",
            desc="The signal IP, emitted when the stream (or each substream) ends.",
            required=True,
        ),
    ],
    config=Config,
)


class OnStreamEnd(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _signal_ip(self, count: int) -> Any:
        out_ip = fbp_capnp.IP.new_message()
        if self.config.as_type == "value":
            out_ip.content = values.value_from_python(self.config.content)
            out_ip.sysAttributes.contentType = values.VALUE_TYPE
        else:
            out_ip.content = self.config.content
            out_ip.sysAttributes.contentType = "Text (JSON)" if self.config.as_type == "json" else "Text"

        if self.config.count_attr:
            brackets.set_attrs(out_ip, {self.config.count_attr: count})
        return out_ip

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        count = 0
        substream_count = 0
        signals = 0
        per_substream = self.config.trigger_on in ("substream_end", "both")
        at_end = self.config.trigger_on in ("stream_end", "both")

        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if not brackets.is_bracket(in_ip):
                count += 1
                substream_count += 1

            if self.config.forward_input and self.out_ports["out"] is not None:
                if not await self.write_out("out", in_ip):
                    self.out_ports["out"] = None

            if per_substream and brackets.is_close_bracket(in_ip):
                signals += 1
                if not await self.write_out("end", self._signal_ip(substream_count)):
                    return
            if brackets.is_open_bracket(in_ip):
                substream_count = 0

        if at_end:
            signals += 1
            _ = await self.write_out("end", self._signal_ip(count))

        logger.info("%s process finished after %d IP(s), emitted %d signal(s)", self.name, count, signals)


def main():
    process.run_process_from_metadata_and_cmd_args(OnStreamEnd(METADATA), METADATA)


if __name__ == "__main__":
    main()
