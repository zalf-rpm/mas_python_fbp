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
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    no_of_ips: int = Field(
        0,
        description="""Number of IPs to wrap into substream. 0 means wrap all IPs and wait for upstream port to close.
        An incoming substream counts as one IP!""",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="e8206aa9-2254-40f0-b2c0-06d31fb25629",
        name="Wrap IPs into substream",
        description="""Wrap a set of incoming IPs into a substream.""",
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
            desc="The IPs to wrap int substream.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="Substream wrapped IPs received on 'in' port.",
        ),
    ],
    config=Config,
)


class WrapIntoSubstream(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        async def open_substream():
            open_ip = fbp_capnp.IP.new_message(type="openBracket")
            if not (error := await self.write_out("out", open_ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        async def close_substream():
            close_ip = fbp_capnp.IP.new_message(type="closeBracket")
            if not (error := await self.write_out("out", close_ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        async def send_ip(ip):
            if not (error := await self.write_out("out", ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        ip_count = self.config.no_of_ips
        collect_all = self.config.no_of_ips == 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                # upstream closed, so send an close bracket
                if not await close_substream():
                    break
                self.in_ports["in"] = None
                logger.info("%s: done received on 'in' port. Process finished", self.name)
                break

            # open substream after first IP received
            if ip_count == 0:
                if not await open_substream():
                    break

            # forward any received IP downstream
            if not await send_ip(in_ip):
                break

            ip_count += 1

            # count whole substream as one IP
            if in_ip.type == "openBracket":
                # keep track of nested substreams
                nesting_level = 1
                while True:
                    # receive next substream IP
                    in_ip = await self.read_in("in")
                    if in_ip is None:
                        logger.info("%s: done received on 'in' port. Closing substream(s).", self.name)
                        # try to close unbalanced substreams if 'in' port suddenly failed
                        for i in range(nesting_level):
                            await close_substream()
                        break

                    # forward any received substream IPs
                    if not await send_ip(in_ip):
                        break

                    # count nesting levels
                    if in_ip.type == "openBracket":
                        nesting_level += 1
                    elif in_ip.type == "closeBracket":
                        nesting_level -= 1
                        # break if toplevel substream closed
                        if nesting_level == 0:
                            break

            if not collect_all and ip_count == self.config.no_of_ips:
                if not await close_substream():
                    break
                ip_count = 0

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WrapIntoSubstream(METADATA), METADATA)


if __name__ == "__main__":
    main()
