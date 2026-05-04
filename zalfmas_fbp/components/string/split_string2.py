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
        logger.info("%s process running", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            s = in_msg.content.as_text()
            logger.info("%s received: %s", self.name, s)
            vals = s.rstrip().split(self.config["split_at"].t)

            for val in vals:
                out_ip = fbp_capnp.IP.new_message(content=val)
                if not await self.write_out("out", out_ip):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent: %s", self.name, val)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(SplitString(meta), meta)


if __name__ == "__main__":
    main()
