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
from typing import TYPE_CHECKING, Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

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
        description=(
            "Forward IPs across multiple outputs using a configurable distribution strategy. "
            "A substream is treated as one unit of work: its whole contents go to a single output, "
            "nested substreams included, so a group is never torn across workers. That means "
            "bracketed input is distributed per substream, not per IP - to parallelise the "
            "individual IPs of a substream instead, strip the brackets before this component (see "
            "'Split bracketed stream') and reassemble them afterwards (see 'Wrap IPs into "
            "substream')."
        ),
    ),
    type="process",
    inPorts=[
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

    async def _forward_substream(self, open_ip: IPReader, strategy: process.ArrayOutStrategy | str) -> bool:
        """Send a whole substream to one output.

        A substream is one unit of work, so distributing its IPs individually would tear a group
        across workers and leave every branch with a malformed stream. Nested substreams go with
        their parent.
        """
        index = await self.choose_array_out_index("out", strategy)
        if index is None:
            return False

        if not await self.write_array_out_at("out", index, open_ip):
            return False

        depth = 1
        while depth > 0:
            in_ip = await self.read_in("in")
            if in_ip is None:
                logger.warning("%s: input closed mid-substream; %d bracket(s) left open.", self.name, depth)
                self.in_ports["in"] = None
                return False
            if brackets.is_open_bracket(in_ip):
                depth += 1
            elif brackets.is_close_bracket(in_ip):
                depth -= 1
            if not await self.write_array_out_at("out", index, in_ip):
                return False
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        strategy = self._distribution_strategy()

        while self.in_ports["in"] and any(self.array_out_ports["out"]):
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            if brackets.is_open_bracket(in_ip):
                if not await self._forward_substream(in_ip, strategy):
                    break
                continue

            if brackets.is_close_bracket(in_ip):
                logger.warning("%s: close-bracket outside a substream; dropping it.", self.name)
                continue

            if not await self.write_array_out("out", strategy, in_ip):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(LoadBalancer(METADATA), METADATA)


if __name__ == "__main__":
    main()
