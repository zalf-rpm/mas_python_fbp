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
import os
import sys
from typing import TYPE_CHECKING, Any, override

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path = [path for path in sys.path if os.path.abspath(path or os.getcwd()) != script_dir]

from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "ip", "name": "IP (Flow packages)"},
    "component": {
        "info": {
            "id": "b1e875af-4ee7-4937-8824-17d185216ec4",
            "name": "copy",
            "description": "Copy IP to multiple outputs.",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "AnyPointer", "desc": "The IP to copy to all attached outports"},
        ],
        "outPorts": [
            {
                "name": "out",
                "type": "array",
                "contentType": "AnyPointer",
                "desc": "Copied IP for each attached outport",
            },
        ],
    },
}


class Copy(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while any(self.array_out_ports["out"]):
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            out_ip = _copy_ip(in_ip)
            if not await self.write_array_out("out", process.ArrayOutStrategy.BROADCAST, out_ip):
                break

        logger.info("%s process finished", self.name)


def _copy_ip(in_ip: IPReader) -> IPBuilder:
    out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
    common.copy_and_set_fbp_attrs(in_ip, out_ip)
    return out_ip


def main():
    process.run_process_from_metadata_and_cmd_args(Copy(meta), meta)


if __name__ == "__main__":
    main()
