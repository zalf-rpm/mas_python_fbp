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
from collections import deque
from typing import TYPE_CHECKING, Literal, override

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
    mode: Literal["first_n", "skip_n", "every_nth", "last_n", "while_true", "until_true"] = Field(
        default="first_n",
        description=(
            "Which IPs to keep: the first n, everything after the first n, every nth, the last n, "
            "everything up to the first IP failing the predicate, or everything up to the first "
            "one passing it."
        ),
    )
    n: int = Field(default=1, description="How many, or the step for 'every_nth'.")
    predicate: selectors.Predicate | None = Field(
        default=None,
        description="The test for 'while_true' and 'until_true'. Same form as 'Filter IPs'.",
    )
    scope: Literal["stream", "substream"] = Field(
        default="stream",
        description="Count over the whole stream, or restart the count in each substream.",
    )
    path_separator: str = Field(default="/", description="Separator used inside selectors.")
    content_type: str | None = Field(
        default=None,
        description="Content type to assume for IPs that carry none, when a selector reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType; '*' covers all.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="48315511-87cb-4e86-a886-0ea28da2c500",
        name="Take or drop IPs",
        description=(
            "Keep part of a stream by position or by a predicate. Substream transparent; the count "
            "can restart per substream."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to select from.", required=True)],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="The kept IPs.", required=True),
        meta.Port(
            name="rej",
            contentType="AnyPointer",
            desc="The IPs not kept. Optional; they are dropped if it is unconnected.",
        ),
    ],
    config=Config,
)


class TakeDropIPs(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._count: int = 0
        self._stopped: bool = False

    def _passes(self, in_ip: IPReader) -> bool:
        if self.config.predicate is None:
            return False
        return selectors.evaluate(
            in_ip,
            self.config.predicate,
            separator=self.config.path_separator,
            content_type=self.config.content_type,
            attr_types=self.config.attr_types,
        )

    def _keeps(self, in_ip: IPReader) -> bool:
        """Whether this IP is kept. Called once per standard IP, in order."""
        mode = self.config.mode
        index = self._count
        self._count += 1

        if mode == "first_n":
            return index < self.config.n
        if mode == "skip_n":
            return index >= self.config.n
        if mode == "every_nth":
            step = max(1, self.config.n)
            return index % step == 0
        if mode == "while_true":
            if self._stopped or not self._passes(in_ip):
                self._stopped = True
                return False
            return True
        if mode == "until_true":
            if self._stopped or self._passes(in_ip):
                self._stopped = True
                return False
            return True
        return False  # last_n is decided at the end, not here

    def _reset_scope(self) -> None:
        self._count = 0
        self._stopped = False

    async def _write(self, port: str, in_ip: IPReader) -> bool:
        if port == "rej" and self.out_ports["rej"] is None:
            return True
        return await self.write_out(port, in_ip)

    async def _run_last_n(self) -> None:
        """Only the tail can be known at the end, so it is buffered; everything else streams."""
        kept: deque[IPReader] = deque(maxlen=max(1, self.config.n))
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if self.config.scope == "substream" and brackets.is_close_bracket(in_ip):
                    for buffered in kept:
                        if not await self._write("out", buffered):
                            return
                    kept.clear()
                if not await self.write_out("out", in_ip):
                    return
                continue

            if len(kept) == kept.maxlen and kept and not await self._write("rej", kept[0]):
                return
            kept.append(in_ip)

        for buffered in kept:
            if not await self._write("out", buffered):
                return

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.config.mode in ("while_true", "until_true") and self.config.predicate is None:
            logger.error("%s: mode %r needs a predicate.", self.name, self.config.mode)
            return

        if self.config.mode == "last_n":
            await self._run_last_n()
            logger.info("%s process finished", self.name)
            return

        kept = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if self.config.scope == "substream" and brackets.is_open_bracket(in_ip):
                    self._reset_scope()
                if not await self.write_out("out", in_ip):
                    break
                continue

            if self._keeps(in_ip):
                kept += 1
                if not await self._write("out", in_ip):
                    break
            elif not await self._write("rej", in_ip):
                break

        logger.info("%s process finished, kept %d IP(s)", self.name, kept)


def main():
    process.run_process_from_metadata_and_cmd_args(TakeDropIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
