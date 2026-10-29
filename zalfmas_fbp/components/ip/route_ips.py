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

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    routes: list[selectors.Predicate] = Field(
        default_factory=list,
        description=(
            "One predicate per slot of the 'out' array port, in order: an IP matching routes[i] "
            "goes to out[i]. Same form as 'Filter IPs'."
        ),
    )
    first_match_only: bool = Field(
        True,
        description="Send each IP to the first matching output only. False broadcasts it to every match.",
    )
    path_separator: str = Field("/", description="Separator used inside selectors.")
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none, when a selector reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType, by attribute name.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="d0de1b9f-8f28-4a6c-85c5-8ec0e748d5e4",
        name="Route IPs",
        description=(
            "Send each IP to the output whose predicate it matches - the semantic counterpart to "
            "'load balancer', which distributes by availability rather than by content. Bracket "
            "IPs are broadcast to every output so each branch sees well-formed substreams."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to route.", required=True)],
    outPorts=[
        meta.Port(
            name="out",
            type="array",
            contentType="AnyPointer",
            desc="Slot i receives the IPs matching routes[i].",
            required=True,
        ),
        meta.Port(
            name="default",
            contentType="AnyPointer",
            desc="IPs matching no route. Optional; they are dropped if it is unconnected.",
        ),
    ],
    config=Config,
)


class RouteIPs(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _matching_routes(self, in_ip: IPReader) -> list[int]:
        matches: list[int] = []
        for index, predicate in enumerate(self.config.routes):
            if selectors.evaluate(
                in_ip,
                predicate,
                separator=self.config.path_separator,
                content_type=self.config.content_type,
                attr_types=self.config.attr_types,
            ):
                matches.append(index)
                if self.config.first_match_only:
                    break
        return matches

    async def _broadcast_bracket(self, in_ip: IPReader) -> bool:
        """Every branch needs the brackets, or its substreams are malformed."""
        for index in range(len(self.array_out_ports["out"])):
            if not await self.write_array_out_at("out", index, in_ip):
                logger.debug("%s: out[%d] is gone; not routing to it any more.", self.name, index)
        if self.out_ports["default"] is not None and not await self.write_out("default", in_ip):
            self.out_ports["default"] = None
        return True

    async def _to_default(self, in_ip: IPReader) -> bool:
        if self.out_ports["default"] is None:
            return True
        return await self.write_out("default", in_ip)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        slots = len(self.array_out_ports["out"])
        if len(self.config.routes) > slots:
            logger.warning(
                "%s has %d route(s) but only %d connected output(s); the extra routes never fire.",
                self.name,
                len(self.config.routes),
                slots,
            )
        if not self.config.routes:
            logger.warning("%s has no routes configured; every IP goes to 'default'.", self.name)

        routed = 0
        defaulted = 0
        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self._broadcast_bracket(in_ip):
                    break
                continue

            matches = [index for index in self._matching_routes(in_ip) if index < slots]
            if not matches:
                defaulted += 1
                if not await self._to_default(in_ip):
                    break
                continue

            routed += 1
            for index in matches:
                if not await self.write_array_out_at("out", index, in_ip):
                    logger.debug("%s: out[%d] is gone; not routing to it any more.", self.name, index)

        logger.info(
            "%s process finished, routed %d IP(s) and sent %d to 'default'",
            self.name,
            routed,
            defaulted,
        )


def main():
    process.run_process_from_metadata_and_cmd_args(RouteIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
