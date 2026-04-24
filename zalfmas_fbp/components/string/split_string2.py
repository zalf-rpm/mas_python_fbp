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
from typing import Any, override

import capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.process as process

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

meta = {
    "category": {"id": "string", "name": "String"},
    "component": {
        "info": {
            "id": "d44040ab-7d5a-44d1-94e8-3f79969edbd4",
            "name": "split string2",
            "description": "Splits a string along delimiter.",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "Text"},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "Text"}],
        "defaultConfig": {"split_at": {"value": ",", "type": "string", "desc": "split string at this character"}},
    },
}


class SplitString(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        await self.process_started()
        logger.info(f"{self.name} process started")

        while True:
            in_port = self.input_port("in")
            out_port = self.output_port("out")
            if not in_port or not out_port:
                break
            if self.is_canceled():
                break
            try:
                in_msg = await in_port.read()
                if in_msg.which() == "done":
                    self.close_input_port("in")
                    continue

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                logger.info(f"{self.name} received: {s}")
                s = s.rstrip()
                vals = s.split(self.config["split_at"].t)

                for val in vals:
                    out_ip = fbp_capnp.IP.new_message(content=val)
                    await out_port.write(value=out_ip)
                    logger.info(f"{self.name} sent: {val}")

            except capnp.KjException as e:
                logger.error(f"{self.name} RPC Exception: {e.description}")
                if e.type in ["DISCONNECTED"]:
                    break

        logger.info(f"{self.name} process finished")
        await self.process_stopped()


def main():
    process.run_process_from_metadata_and_cmd_args(SplitString(meta), meta)


if __name__ == "__main__":
    main()
