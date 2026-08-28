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

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    remove_attrs: list[str] = Field(
        default_factory=list,
        description='["attr1", "attr2"] -> Names of the attributes to remove from incoming IPs.',
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="f07fa00e-dd80-45da-bc49-397d5e25fe0f",
        name="remove attributes",
        description="Remove the configured attributes from incoming IPs and forward them otherwise unchanged.",
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
            desc="Input message with any content and attributes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The same message as on 'in', but with the configured attributes removed.",
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

        remove = set(self.config.remove_attrs)

        while self.in_ports["in"] and self.out_ports["out"]:
            try:
                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    continue

                # Forward stream brackets (and anything else without content
                # attributes) unchanged to keep substream structure intact.
                if in_ip.type in ("openBracket", "closeBracket"):
                    if not await self.write_out("out", in_ip):
                        logger.info("%s process finished", self.name)
                        return
                    continue

                out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
                out_ip.attributes = [  # pyright: ignore
                    attr for attr in in_ip.attributes if attr.key not in remove
                ]
                if not await self.write_out("out", out_ip):
                    logger.info("%s process finished", self.name)
                    return

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
