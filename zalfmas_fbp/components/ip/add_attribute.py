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
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.common import brackets, values
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
        id="1d442f41-dee4-4973-ad99-09855af1d7ad",
        name="add attribute",
        description=(
            "Add attribute to incoming IP. Substream transparent: bracket IPs are forwarded "
            "unchanged and do not consume an IP from the 'attr' port, so the pairing of "
            "attributes to IPs is unaffected by grouping."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="Arbitrary content.",
        ),
        meta.Port(
            name="attr",
            contentType="AnyPointer",
            desc="Arbitrary content to store as attached attribute with name 'to_attr'.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            desc="IP (from in port) and attribute 'to_attr' containing content from attr port.",
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

        attr = None
        attr_type: str | None = None
        while self.in_ports["in"] and (self.in_ports["attr"] or attr) and self.out_ports["out"]:
            try:
                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    continue

                # Brackets are the caller's grouping, not data: forwarding them before touching
                # 'attr' keeps one attr IP paired with one standard IP, which consuming one per
                # bracket would slip.
                if brackets.is_bracket(in_ip):
                    if not await self.write_out("out", in_ip):
                        logger.info("%s: Could not send IP. Process finished.", self.name)
                        return
                    continue

                if self.in_ports["attr"]:
                    attr_ip = await self.read_in("attr")
                    if attr_ip is None:
                        # The 'attr' port closing must not take this IP with it: the loop is
                        # allowed to run on with the last attribute, so dropping it here lost
                        # one input IP every time the attr side finished first.
                        self.in_ports["attr"] = None
                        if attr is None:
                            continue
                    else:
                        attr = attr_ip.content
                        # carried over so the attribute can be read back: an attribute written
                        # without a type resolves to MISSING downstream (D4)
                        attr_type = values.content_type_of(attr_ip)

                out_ip = common.copy_ip(in_ip)
                brackets.copy_attrs(in_ip, out_ip, extra={self.config.to_attr: brackets.Attr(attr, attr_type)})
                if not await self.write_out("out", out_ip):
                    logger.info("%s: Could not send IP. Process finished.", self.name)
                    return

            except capnp.KjException as e:
                logger.exception("%s: %s RPC Exception: %s", Path(__file__).name, self.name, e.description)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
