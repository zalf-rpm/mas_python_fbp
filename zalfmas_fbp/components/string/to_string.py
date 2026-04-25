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
            "id": "250488d8-7519-49a8-820e-0e981ffb2a71",
            "name": "to string",
            "description": "Outputs input structures as string (if possible).",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "AnyStruct"},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "Text"}],
        "defaultConfig": {
            "struct_type": {
                "value": None,
                "type": "string",
                "desc": "A loadable Cap'n Proto schema and the contained struct to parse the 'in' content to.",
            }
        },
    },
}


class ToString(process.Process):
    def __init__(self, metadata, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    async def run(self):
        await self.process_started()
        logger.info("%s process started", self.name)

        if self.config["struct_type"] is None:
            t = None
        else:
            try:
                t, _ = common.load_capnp_module(self.config["struct_type"].t)
            except Exception as e:
                logger.error("Failed to load Cap'n Proto module: %s", e)
                t = None

        while self.in_ports["in"] and self.out_ports["out"]:
            if self.is_canceled():
                break
            try:
                in_port = self.in_ports["in"]
                out_port = self.out_ports["out"]
                if not in_port or not out_port:
                    break

                in_msg = await in_port.read()
                if in_msg.which() == "done":
                    self.in_ports["in"] = None
                    continue

                c = in_msg.value.as_struct(fbp_capnp.IP).content
                if t:
                    c = c.as_struct(t)
                logger.info("%s received: %s", self.name, c)

                c_str = str(c)
                c_str = c_str.replace("<", "")
                c_str = c_str.replace(">", "")
                out_ip = fbp_capnp.IP.new_message(content=c_str)
                await out_port.write(value=out_ip)
                logger.info("%s sent: %s", self.name, c_str)

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break

        logger.info("%s process finished", self.name)
        await self.process_stopped()


def main():
    process.run_process_from_metadata_and_cmd_args(ToString(meta), meta)


if __name__ == "__main__":
    main()
