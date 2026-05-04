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
from typing import Any

from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

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
            },
        },
    },
}


class ToString(process.Process):
    def __init__(self, metadata, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    async def run(self):
        logger.info("%s process running", self.name)

        struct_type = self.config.get("struct_type")
        if struct_type is None:
            t = None
        else:
            try:
                t: Any
                t, _ = common.load_capnp_module(struct_type.t)
            except (AttributeError, ImportError, RuntimeError, TypeError, ValueError) as e:
                logger.error("Failed to load Cap'n Proto module: %s", e)
                t = None

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            c = in_msg.content
            if t:
                c = c.as_struct(t)
            logger.info("%s received: %s", self.name, c)

            c_str = str(c)
            c_str = c_str.replace("<", "")
            c_str = c_str.replace(">", "")
            out_ip = fbp_capnp.IP.new_message(content=c_str)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return
            logger.info("%s sent: %s", self.name, c_str)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(ToString(meta), meta)


if __name__ == "__main__":
    main()
