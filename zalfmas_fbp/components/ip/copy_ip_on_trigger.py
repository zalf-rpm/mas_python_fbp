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
from typing import TYPE_CHECKING, override

from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    pass


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="f3db87d1-7a50-491c-95aa-a2b6f17fb4f5",
        name="Copy IP on trigger",
        description="Copy IP to multiple outputs when triggered.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="The IP to copy to all attached outports.",
        ),
        meta.Port(
            name="trigger",
            contentType="AnyPointer",
            desc="If connected, then a copy will only be triggered if an IP arrives on this port.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            type="array",
            contentType="AnyPointer",
            desc="Copied IP for each attached outport",
        ),
        meta.Port(
            name="trigger",
            contentType="AnyPointer",
            desc="If connected just forwards the IP which triggered the copies.",
        ),
    ],
    config=Config,
)


class CopyOnTrigger(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.in_ports["trigger"] and self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                logger.info("%s: done received on 'in' port, therefore nothing to copy. Process finished", self.name)
                return

        while self.in_ports["trigger"] and any(self.array_out_ports["out"]):
            trigger_ip = await self.read_in("trigger")
            if trigger_ip is None:
                break

            # out_ip = copy_ip(in_ip)
            if not await self.write_array_out("out", process.ArrayOutStrategy.BROADCAST, in_ip):  # out_ip):
                break

            if self.out_ports["trigger"]:
                await self.write_out("trigger", trigger_ip)

        logger.info("%s: process finished", self.name)


def copy_ip(in_ip: IPReader | IPBuilder) -> IPBuilder:
    out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
    out_ip.attributes = list(in_ip.attributes)
    return out_ip


def main():
    process.run_process_from_metadata_and_cmd_args(CopyOnTrigger(METADATA), METADATA)


if __name__ == "__main__":
    main()
