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
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class SplitStringConfig(process.ProcessConfig):
    split_at: str = Field(default=",", description="split string at this character")
    wrap_in_substream: bool = Field(
        default=False,
        description=(
            "Wrap each input string's parts in an open-/close-bracket pair, so one input becomes "
            "one substream. Off by default, which means the parts of successive inputs arrive as "
            "one flat stream - the same shape as if the strings had arrived separately."
        ),
    )
    keep_empty: bool = Field(
        default=True,
        description="Emit empty parts as well, e.g. the two produced by splitting 'a,,b' at ','.",
    )


METADATA = meta.Component(
    category=meta.Category(id="string", name="String"),
    info=meta.Info(
        id="d44040ab-7d5a-44d1-94e8-3f79969edbd4",
        name="split string",
        description=(
            "Splits a string along a delimiter, emitting one IP per part. Substream transparent: "
            "brackets around the incoming strings are forwarded unchanged, so an input substream "
            "stays one substream. Turn on 'wrap_in_substream' to additionally make each input's "
            "parts their own substream - nested inside any incoming one."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType="Text", desc="Strings to split.", required=True),
    ],
    outPorts=[
        meta.Port(name="out", contentType="Text", desc="One IP per part.", required=True),
    ],
    config=SplitStringConfig,
)


class SplitString(process.Process[SplitStringConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_msg = await self.read_in("in")
            if in_msg is None:
                self.in_ports["in"] = None
                break

            # Incoming grouping is the caller's; it is forwarded so an input substream stays one
            # substream, whatever this component does inside it.
            if brackets.is_bracket(in_msg):
                if not await self.write_out("out", in_msg):
                    logger.info("%s process finished", self.name)
                    return
                continue

            text = in_msg.content.as_text()
            parts = text.rstrip().split(self.config.split_at)
            if not self.config.keep_empty:
                parts = [part for part in parts if part != ""]
            logger.info("%s received %r, split into %d part(s)", self.name, text, len(parts))

            if self.config.wrap_in_substream and not await self.write_out(
                "out",
                brackets.make_bracket("openBracket"),
            ):
                return

            for part in parts:
                out_ip = fbp_capnp.IP.new_message(content=part)
                brackets.copy_attrs(in_msg, out_ip)
                if not await self.write_out("out", out_ip):
                    logger.info("%s process finished", self.name)
                    return

            if self.config.wrap_in_substream and not await self.write_out(
                "out",
                brackets.make_bracket("closeBracket"),
            ):
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(SplitString(METADATA), METADATA)


if __name__ == "__main__":
    main()
