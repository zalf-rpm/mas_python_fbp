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

import capnp
from zalfmas_capnp_schemas_with_stubs import common_capnp, fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.process as process

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class SplitString(process.Process):
    def __init__(self, con_man: common.ConnectionManager = None):
        process.Process.__init__(self, con_man)
        self.name = "split string"
        self.description = "Split a string."
        self.in_ports_config = {"in": "text"}
        self.out_ports_config = {"in": "text"}
        self.config["split_at"]: dict[str, common_capnp.ValueBuilder] = (
            common_capnp.Value.new_message(t=",")
        )

    async def run(self):
        await self.process_started()
        logger.info(f"{self.name} process started")

        while self.isp["in"] and self.ops["out"]:
            if self.is_canceled():
                break
            try:
                in_msg = await self.ips["in"].read()
                if in_msg.which() == "done":
                    self.ips["in"] = None
                    continue

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                logger.info(f"{self.name} received: {s}")
                s = s.rstrip()
                vals = s.split(self.config["split_at"].t)

                for val in vals:
                    out_ip = fbp_capnp.IP.new_message(content=val)
                    await self.ops["out"].write(value=out_ip)
                    logger.info(f"{self.name} sent: {val}")

            except capnp.KjException as e:
                logger.error(f"{self.name} RPC Exception: {e.description()}")
                if e.type in ["DISCONNECTED"]:
                    break

        logger.info(f"{self.name} process finished")
        await self.process_stopped()


def main():
    process.parse_cmd_args_and_serve_process(SplitString())


if __name__ == "__main__":
    main()
