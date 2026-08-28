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

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

SUBSTREAM_LENGTH_ATTR = "substream_length"
SUBSTREAM_LENGTH_VALUE_TYPE = "@0xe17592335373b246 = common/common.capnp:Value"


class Config(process.ProcessConfig):
    pass


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="9552eb98-90ae-4c4e-88aa-b91a2bc7dee9",
        name="Split bracketed stream",
        description="Split a bracketed stream into its non-bracket IPs and its bracket IPs. "
        "Assumes a single level of bracket nesting.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="Input stream, potentially containing open-/close-bracket IPs wrapping non-bracket IPs.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The non-bracket IPs received on 'in', forwarded unchanged.",
        ),
        meta.Port(
            name="brackets",
            contentType="AnyPointer",
            desc=(
                "The open-/close-bracket IPs received on 'in'. The close-bracket IP carries an additional "
                f"'{SUBSTREAM_LENGTH_ATTR}' attribute (common.capnp:Value(ui64)) counting the non-bracket IPs "
                "forwarded on 'out' for that substream."
            ),
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

        count = 0
        while self.in_ports["in"] and self.out_ports["out"] and self.out_ports["brackets"]:
            try:
                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    continue

                if in_ip.type == "openBracket":
                    count = 0
                    if not await self.write_out("brackets", in_ip):
                        logger.info("%s process finished", self.name)
                        return

                elif in_ip.type == "closeBracket":
                    out_ip = fbp_capnp.IP.new_message(type="closeBracket")
                    attrs = out_ip.init("attributes", len(in_ip.attributes) + 1)
                    for i, attr in enumerate(in_ip.attributes):
                        attrs[i].key = attr.key
                        attrs[i].desc = attr.desc
                        attrs[i].value = attr.value
                        attrs[i].valueType = attr.valueType
                    attrs[len(in_ip.attributes)].key = SUBSTREAM_LENGTH_ATTR
                    attrs[len(in_ip.attributes)].value = common_capnp.Value.new_message(ui64=count)
                    attrs[len(in_ip.attributes)].valueType = SUBSTREAM_LENGTH_VALUE_TYPE

                    count = 0
                    if not await self.write_out("brackets", out_ip):
                        logger.info("%s process finished", self.name)
                        return

                else:
                    if not await self.write_out("out", in_ip):
                        logger.info("%s process finished", self.name)
                        return
                    count += 1

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
