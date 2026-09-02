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

import asyncio
import logging
from typing import override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

logger = logging.getLogger(__name__)


class CounterConfig(process.ProcessConfig):
    start_at: int = Field(
        0,
        description="The value the counter starts (and resets) at.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="simple",
        name="Simple",
    ),
    info=meta.Info(
        id="b96e9b2a-f5c6-44d2-a6a9-aec767f1bda9",
        name="counter",
        description="Counts up and sends the count on 'count'. 'reset' triggers a reset to 'start_at'.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="reset",
            contentType="AnyPointer",
            desc="Received IPs are discarded, they only trigger a reset of the counter to 'start_at'.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="count",
            contentType="@0xe17592335373b246 = common/common.capnp:Value.i64",
            desc="The current count.",
        ),
    ],
    config=CounterConfig,
)


class Counter(process.Process[CounterConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        count = self.config.start_at

        async def send_counts():
            nonlocal count
            while self.out_ports["count"]:
                sent = count
                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(i64=sent))
                if not await self.write_out("count", out_ip):
                    break
                # a reset may have landed while the write above was in flight; only advance
                # if nothing else already changed count in the meantime
                if count == sent:
                    count = sent + 1

        async def watch_reset():
            nonlocal count
            while self.in_ports["reset"]:
                reset_ip = await self.read_in("reset")
                if reset_ip is None:
                    self.in_ports["reset"] = None
                    break
                count = self.config.start_at
                logger.info("%s: reset to %d", self.name, count)

        tasks = [send_counts()]
        if self.in_ports["reset"]:
            tasks.append(watch_reset())
        await asyncio.gather(*tasks)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Counter(METADATA), METADATA)


if __name__ == "__main__":
    main()
