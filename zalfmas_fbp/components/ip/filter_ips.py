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
from typing import TYPE_CHECKING, Any, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    predicate: selectors.Predicate = Field(
        default_factory=lambda: selectors.Predicate(left=".", op=selectors.Op.TRUTHY),
        description=(
            "The test each IP has to pass. A leaf is left/op/right, where left and right are "
            "selectors ('@attr', './path', '#type') or literals; 'all', 'any' and 'not' nest them."
        ),
    )
    invert: bool = Field(False, description="Send IPs that pass to 'rej' and the rest to 'out'.")
    drop_empty_substreams: bool = Field(
        True,
        description=(
            "Suppress a substream's brackets on an output that received none of its IPs, rather "
            "than emitting an empty substream. Nesting is preserved either way."
        ),
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
        id="3e52dd18-7369-4701-9a9a-7102e7fae1e2",
        name="Filter IPs",
        description=(
            "Forward IPs that pass a predicate and divert the rest. Substream sensitive: a "
            "substream left empty on an output has its brackets suppressed."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to test.", required=True)],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="IPs the predicate accepted.", required=True),
        meta.Port(
            name="rej",
            contentType="AnyPointer",
            desc="IPs the predicate rejected. Optional; they are dropped if it is unconnected.",
        ),
    ],
    config=Config,
)


class _BracketGate:
    """Holds a substream's open-brackets back until something is written through them.

    One per output port, so `out` and `rej` each keep well-formed substreams containing only the
    IPs that went their way, and neither emits a substream that turned out to be empty.
    """

    def __init__(self, write: Callable[[Any], Awaitable[bool]], gated: bool) -> None:
        self._write: Callable[[Any], Awaitable[bool]] = write
        self._gated: bool = gated
        self._pending: list[list[Any]] = []

    async def open_bracket(self, ip: IPReader) -> bool:
        if not self._gated:
            return await self._write(ip)
        self._pending.append([ip, False])
        return True

    async def _flush(self) -> bool:
        for entry in self._pending:
            if not entry[1]:
                if not await self._write(entry[0]):
                    return False
                entry[1] = True
        return True

    async def ip(self, ip: IPReader) -> bool:
        if self._gated and not await self._flush():
            return False
        return await self._write(ip)

    async def close_bracket(self, ip: IPReader) -> bool:
        if not self._gated:
            return await self._write(ip)
        if not self._pending:
            # Unbalanced close; forward it so the imbalance stays visible downstream.
            return await self._write(ip)
        _open_ip, emitted = self._pending.pop()
        return await self._write(ip) if emitted else True


class FilterIPs(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _accepts(self, in_ip: IPReader) -> bool:
        result = selectors.evaluate(
            in_ip,
            self.config.predicate,
            separator=self.config.path_separator,
            content_type=self.config.content_type,
            attr_types=self.config.attr_types,
        )
        return not result if self.config.invert else result

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        async def write_out(ip: Any) -> bool:
            return await self.write_out("out", ip)

        async def write_rej(ip: Any) -> bool:
            # An unconnected 'rej' means "drop", which is not a failure.
            return await self.write_out("rej", ip) if self.out_ports["rej"] is not None else True

        gated = self.config.drop_empty_substreams
        accepted = _BracketGate(write_out, gated)
        rejected = _BracketGate(write_rej, gated)
        counts = {"accepted": 0, "rejected": 0}

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_open_bracket(in_ip):
                if not (await accepted.open_bracket(in_ip) and await rejected.open_bracket(in_ip)):
                    break
                continue
            if brackets.is_close_bracket(in_ip):
                if not (await accepted.close_bracket(in_ip) and await rejected.close_bracket(in_ip)):
                    break
                continue

            if self._accepts(in_ip):
                counts["accepted"] += 1
                if not await accepted.ip(in_ip):
                    break
            else:
                counts["rejected"] += 1
                if not await rejected.ip(in_ip):
                    break

        logger.info(
            "%s process finished, accepted %d and rejected %d IP(s)",
            self.name,
            counts["accepted"],
            counts["rejected"],
        )


def main():
    process.run_process_from_metadata_and_cmd_args(FilterIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
