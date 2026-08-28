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
from typing import TYPE_CHECKING, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.process.task_utils import wait_for_tasks_or_stop

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

SUBSTREAM_LENGTH_ATTR = "substream_length"


class Config(process.ProcessConfig):
    no_of_ips: int = Field(
        0,
        description="""Number of IPs to wrap into substream. 0 means wrap all IPs and wait for upstream port to close.
        An incoming substream counts as one IP!""",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="e8206aa9-2254-40f0-b2c0-06d31fb25629",
        name="Wrap IPs into substream",
        description="""Wrap a set of incoming IPs into a substream.""",
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
            desc="The IPs to wrap int substream.",
        ),
        meta.Port(
            name="brackets",
            contentType="AnyPointer",
            desc=(
                "Optional side-channel of open-/close-bracket IPs (e.g. from a 'Split bracketed stream' "
                "component) dictating substream boundaries. If unconnected, 'no_of_ips' controls wrapping "
                "as usual. If connected, an open-bracket received here is forwarded to 'out', 'in' IPs are "
                "then forwarded until a matching close-bracket is received here; its 'substream_length' "
                "(common.capnp:Value(ui64)) attribute determines how many 'in' IPs belong to the substream."
            ),
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="Substream wrapped IPs received on 'in' port.",
        ),
    ],
    config=Config,
)


class WrapIntoSubstream(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.in_ports["brackets"] is not None:
            await self._run_bracket_synced()
        else:
            await self._run_free_running()

        logger.info("%s: process finished", self.name)

    async def _run_free_running(self):
        async def open_substream():
            open_ip = fbp_capnp.IP.new_message(type="openBracket")
            if not (error := await self.write_out("out", open_ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        async def close_substream():
            close_ip = fbp_capnp.IP.new_message(type="closeBracket")
            if not (error := await self.write_out("out", close_ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        async def send_ip(ip):
            if not (error := await self.write_out("out", ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        ip_count = 0
        collect_all = self.config.no_of_ips == 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                # upstream closed, so send an close bracket
                if not await close_substream():
                    break
                self.in_ports["in"] = None
                logger.info("%s: done received on 'in' port. Process finished", self.name)
                break

            # open substream after first IP received
            if ip_count == 0:
                if not await open_substream():
                    break

            # forward any received IP downstream
            if not await send_ip(in_ip):
                break

            ip_count += 1

            # count whole substream as one IP
            if in_ip.type == "openBracket":
                # keep track of nested substreams
                nesting_level = 1
                while True:
                    # receive next substream IP
                    in_ip = await self.read_in("in")
                    if in_ip is None:
                        logger.info("%s: done received on 'in' port. Closing substream(s).", self.name)
                        # try to close unbalanced substreams if 'in' port suddenly failed
                        for i in range(nesting_level):
                            await close_substream()
                        break

                    # forward any received substream IPs
                    if not await send_ip(in_ip):
                        break

                    # count nesting levels
                    if in_ip.type == "openBracket":
                        nesting_level += 1
                    elif in_ip.type == "closeBracket":
                        nesting_level -= 1
                        # break if toplevel substream closed
                        if nesting_level == 0:
                            break

            if not collect_all and ip_count == self.config.no_of_ips:
                if not await close_substream():
                    break
                ip_count = 0

    async def _run_bracket_synced(self):
        async def send_ip(ip):
            if not (error := await self.write_out("out", ip)):
                self.out_ports["out"] = None
                logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
            return error

        async def read_next_open_bracket():
            while True:
                b_ip = await self.read_in("brackets")
                if b_ip is None:
                    self.in_ports["brackets"] = None
                    return None
                if b_ip.type == "openBracket":
                    return b_ip
                logger.warning(
                    "%s: ignoring IP of type '%s' received on 'brackets' port while waiting for next open-bracket.",
                    self.name,
                    b_ip.type,
                )

        while self.in_ports["brackets"] and self.out_ports["out"]:
            open_ip = await read_next_open_bracket()
            if open_ip is None:
                break

            if not await send_ip(open_ip):
                return

            count = 0
            close_ip = None
            in_task: asyncio.Future | None = None
            brackets_task: asyncio.Future | None = None

            # Race reading 'in' (forward + count each IP) against 'brackets' (watch for the close-bracket).
            while close_ip is None and self.in_ports["in"] and self.in_ports["brackets"]:
                if in_task is None:
                    in_task = asyncio.ensure_future(self.read_in("in"))
                if brackets_task is None:
                    brackets_task = asyncio.ensure_future(self.read_in("brackets"))

                done, stopped = await wait_for_tasks_or_stop({in_task, brackets_task}, self.stop_event)
                if stopped:
                    for task in (in_task, brackets_task):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(in_task, brackets_task, return_exceptions=True)
                    return

                if in_task in done:
                    in_ip = in_task.result()
                    in_task = None
                    if in_ip is None:
                        self.in_ports["in"] = None
                        logger.warning(
                            "%s: 'in' port closed while assembling substream (received %d IP(s)); aborting substream.",
                            self.name,
                            count,
                        )
                    elif not await send_ip(in_ip):
                        return
                    else:
                        count += 1

                if brackets_task in done:
                    b_ip = brackets_task.result()
                    brackets_task = None
                    if b_ip is None:
                        self.in_ports["brackets"] = None
                        logger.warning("%s: 'brackets' port closed while assembling substream.", self.name)
                    elif b_ip.type == "closeBracket":
                        close_ip = b_ip
                    else:
                        logger.warning(
                            "%s: ignoring IP of type '%s' received on 'brackets' port while assembling substream.",
                            self.name,
                            b_ip.type,
                        )

            for task in (in_task, brackets_task):
                if task is not None and not task.done():
                    task.cancel()
            for task in (in_task, brackets_task):
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)

            if close_ip is None:
                # 'in' or 'brackets' closed before a matching close-bracket could be assembled.
                return

            substream_length_val = common.get_fbp_attr(close_ip, SUBSTREAM_LENGTH_ATTR, common_capnp.Value.schema)
            if substream_length_val is None:
                logger.warning(
                    "%s: close-bracket IP on 'brackets' port has no '%s' attribute; assuming %d.",
                    self.name,
                    SUBSTREAM_LENGTH_ATTR,
                    count,
                )
                substream_length = count
            else:
                substream_length = substream_length_val.ui64

            # Blockingly wait on 'in' until substream_length IPs have been forwarded in total.
            while count < substream_length:
                if not self.in_ports["in"]:
                    logger.warning(
                        "%s: 'in' port closed before reaching expected substream_length=%d (received %d).",
                        self.name,
                        substream_length,
                        count,
                    )
                    break
                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    logger.warning(
                        "%s: 'in' port closed before reaching expected substream_length=%d (received %d).",
                        self.name,
                        substream_length,
                        count,
                    )
                    break
                if not await send_ip(in_ip):
                    return
                count += 1

            stripped_attrs = [attr for attr in close_ip.attributes if attr.key != SUBSTREAM_LENGTH_ATTR]
            out_close_ip = fbp_capnp.IP.new_message(type="closeBracket")
            attrs = out_close_ip.init("attributes", len(stripped_attrs))
            for i, attr in enumerate(stripped_attrs):
                attrs[i].key = attr.key
                attrs[i].desc = attr.desc
                attrs[i].value = attr.value
                attrs[i].valueType = attr.valueType

            if not await send_ip(out_close_ip):
                return


def main():
    process.run_process_from_metadata_and_cmd_args(WrapIntoSubstream(METADATA), METADATA)


if __name__ == "__main__":
    main()
