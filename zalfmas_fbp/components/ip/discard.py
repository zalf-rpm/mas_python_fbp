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

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    report_every: int = Field(
        default=0,
        description="Log progress every this many IPs. 0 only reports the total at the end.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="af8fe95a-8040-441a-b4b3-13ee33ddbbcf",
        name="Discard",
        description=(
            "Read a stream and drop it. Needed because leaving an output unconnected and connecting "
            "it to nothing are different things: an upstream component blocks on a connected "
            "channel nobody drains."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="The stream to drain.", required=True)],
    outPorts=[],
    config=Config,
)


class Discard(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.discarded: int = 0
        self.brackets_discarded: int = 0

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                self.brackets_discarded += 1
            else:
                self.discarded += 1
                if self.config.report_every > 0 and self.discarded % self.config.report_every == 0:
                    logger.info("%s: discarded %d IP(s) so far", self.name, self.discarded)

        logger.info(
            "%s process finished, discarded %d IP(s) and %d bracket(s)",
            self.name,
            self.discarded,
            self.brackets_discarded,
        )


def main():
    process.run_process_from_metadata_and_cmd_args(Discard(METADATA), METADATA)


if __name__ == "__main__":
    main()
