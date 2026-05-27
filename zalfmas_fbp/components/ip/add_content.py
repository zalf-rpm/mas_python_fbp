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

import logging
from pathlib import Path
from typing import override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    to_attr: str = Field(
        "attr",
        description="The attribute's name to add to the outgoing message.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="030214e4-7ce8-4de7-8b3c-fb96b7fba7e0",
        name="Add content",
        description="Add content to incoming IP, optionally moving the old to an attribute.",
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
            desc="Arbitrary IP from upstream.",
        ),
        meta.Port(
            name="content",
            contentType="AnyPointer",
            desc="Arbitrary content to exchange for 'in's content. Optionally move 'in's content to attribute 'to_attr'",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="IP (from in port) with new content from 'content' and possibly old content as attribute 'to_attr'.",
        ),
    ],
    config=Config,
)


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        new_content = None
        while self.in_ports["in"] and (self.in_ports["content"] or new_content) and self.out_ports["out"]:
            try:
                if self.in_ports["content"]:
                    content_ip = await self.read_in("content")
                    if content_ip is None:
                        self.in_ports["content"] = None
                        continue
                    new_content = content_ip.content

                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    continue

                out_ip = fbp_capnp.IP.new_message(content=new_content)
                if self.config.to_attr:
                    common.copy_and_set_fbp_attrs(in_ip, out_ip, **{self.config.to_attr: in_ip.content})
                else:
                    common.copy_and_set_fbp_attrs(in_ip, out_ip)
                if not await self.write_out("out", out_ip):
                    logger.info("%s: Could not send IP. Process finished.", self.name)
                    return

            except capnp.KjException as e:
                logger.exception("%s: RPC Exception: %s", Path(__file__).name, e.description())

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
