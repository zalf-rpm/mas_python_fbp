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
from typing import Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.ip.copy_ip import copy_ip
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

logger = logging.getLogger(__name__)


class LoadBalancerConfig(process.ProcessConfig):
    distribution_strategy: Literal["next_available", "round_robin"] = Field(
        "next_available",
        description="Distribution strategy for choosing an output.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="d73056f1-47b5-4ca5-a9ea-c7c5dff89b1d",
        name="load balancer",
        description="Forward IPs across multiple outputs using a configurable distribution strategy.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="The IP to forward to one attached outport",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            type="array",
            contentType="AnyPointer",
            desc="Outgoing IPs distributed one-by-one across attached outports",
        ),
    ],
    config=LoadBalancerConfig,
)


class LoadBalancer(process.Process[LoadBalancerConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _distribution_strategy(self) -> process.ArrayOutStrategy:
        strategy = process.ArrayOutStrategy(self.config.distribution_strategy)
        if strategy == process.ArrayOutStrategy.BROADCAST:
            msg = "load balancer distribution_strategy must be 'round_robin' or 'next_available'"
            raise ValueError(msg)
        return strategy

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        strategy = self._distribution_strategy()

        while any(self.array_out_ports["out"]):
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            out_ip = copy_ip(in_ip)
            if not await self.write_array_out("out", strategy, out_ip):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(LoadBalancer(METADATA), METADATA)


if __name__ == "__main__":
    main()
