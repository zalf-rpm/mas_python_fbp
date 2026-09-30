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

import asyncio
import logging
from collections import deque
from typing import TYPE_CHECKING, Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import selectors
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.process.task_utils import wait_for_tasks_or_stop

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    mode: Literal["pass_n_per_signal", "open_close", "drop_while_closed"] = Field(
        default="pass_n_per_signal",
        description=(
            "'pass_n_per_signal' releases n held IPs per signal. 'open_close' toggles: IPs flow "
            "while open and are held while closed. 'drop_while_closed' discards rather than holds "
            "what arrives while closed."
        ),
    )
    n: int = Field(default=1, description="How many IPs one signal releases in 'pass_n_per_signal'.")
    open_predicate: selectors.Predicate | None = Field(
        default=None,
        description=(
            "For the toggling modes: a signal passing this opens the gate, one failing it closes. "
            "Null makes every signal a toggle."
        ),
    )
    start_open: bool = Field(
        default=False,
        description="Whether the gate starts open, for the toggling modes.",
    )
    max_held: int = Field(
        default=10000,
        description="How many IPs to hold before dropping the oldest. 0 means no limit.",
    )
    path_separator: str = Field(default="/", description="Separator used inside the predicate.")


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="40656b19-8ee0-4f5a-b77f-2df759d89601",
        name="Gate",
        description=(
            "Hold IPs back until a signal releases them. Where 'Copy IP on trigger' copies on "
            "demand, this paces an existing stream."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType="AnyPointer", desc="IPs to pace.", required=True),
        meta.Port(
            name="signal",
            contentType="AnyPointer",
            desc="Signals releasing or toggling. Their content is only read by 'open_predicate'.",
            role="control",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="The released IPs.", required=True),
    ],
    config=Config,
)


class Gate(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._held: deque[IPReader] = deque()
        self._open: bool = False
        self._dropped: int = 0
        # Signals are credits, not "release now": a signal arriving before the IP it is meant to
        # release must still release it, and with two inputs there is no guaranteed arrival order.
        self._credits: int = 0

    def _signal_opens(self, signal: IPReader) -> bool:
        if self.config.open_predicate is None:
            return not self._open  # a bare signal toggles
        return selectors.evaluate(signal, self.config.open_predicate, separator=self.config.path_separator)

    def _hold(self, in_ip: IPReader) -> None:
        self._held.append(in_ip)
        if self.config.max_held > 0:
            while len(self._held) > self.config.max_held:
                _ = self._held.popleft()
                self._dropped += 1

    async def _release(self, how_many: int | None) -> bool:
        """Release up to how_many held IPs, or all of them when None."""
        released = 0
        while self._held and (how_many is None or released < how_many):
            if not await self.write_out("out", self._held.popleft()):
                return False
            released += 1
        return True

    async def _spend_credits(self) -> bool:
        """Release held IPs against outstanding credits."""
        while self._credits > 0 and self._held:
            if not await self.write_out("out", self._held.popleft()):
                return False
            self._credits -= 1
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        self._open = self.config.start_open

        # A started read is a claim on a message, so pending reads are kept across iterations and
        # always consumed rather than being cancelled - see ip/wrap_into_substream for why.
        pending: dict[str, asyncio.Future | None] = {"in": None, "signal": None}

        def start(name: str) -> asyncio.Future:
            task = pending[name]
            if task is None or task.done() and pending[name] is None:
                task = asyncio.ensure_future(self.read_in(name))
                pending[name] = task
            return task

        while self.out_ports["out"] and (self.in_ports["in"] or self._held):
            tasks = set()
            if self.in_ports["in"]:
                tasks.add(start("in"))
            if self.in_ports["signal"]:
                tasks.add(start("signal"))
            if not tasks:
                break

            done, stopped = await wait_for_tasks_or_stop(tasks, self.stop_event)
            if stopped:
                return

            ordered = sorted(done, key=lambda t: 0 if t is pending["in"] else 1)
            for task in ordered:
                name = "in" if task is pending["in"] else "signal"
                pending[name] = None
                in_ip = task.result()

                if in_ip is None:
                    self.in_ports[name] = None
                    continue

                if name == "in":
                    if self._open and self.config.mode != "pass_n_per_signal":
                        if not await self.write_out("out", in_ip):
                            return
                    elif self.config.mode == "drop_while_closed" and not self._open:
                        self._dropped += 1
                    else:
                        self._hold(in_ip)
                        if self.config.mode == "pass_n_per_signal" and not await self._spend_credits():
                            return
                    continue

                if self.config.mode == "pass_n_per_signal":
                    self._credits += max(1, self.config.n)
                    if not await self._spend_credits():
                        return
                else:
                    self._open = self._signal_opens(in_ip)
                    logger.info("%s: gate is now %s", self.name, "open" if self._open else "closed")
                    if self._open and not await self._release(None):
                        return

            if not self.in_ports["in"] and not self.in_ports["signal"]:
                break

        if self._held:
            logger.info("%s: %d IP(s) were still held when the inputs closed.", self.name, len(self._held))
        if self._credits:
            logger.info("%s: %d unused signal credit(s) at the end.", self.name, self._credits)
        if self._dropped:
            logger.warning("%s: dropped %d IP(s).", self.name, self._dropped)
        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Gate(METADATA), METADATA)


if __name__ == "__main__":
    main()
