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
from typing import Any, override

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path = [path for path in sys.path if os.path.abspath(path or os.getcwd()) != script_dir]

from zalfmas_common import common

from zalfmas_fbp.components.ip.copy import copy_ip
from zalfmas_fbp.run import process

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "ip", "name": "IP (Flow packages)"},
    "component": {
        "info": {
            "id": "d73056f1-47b5-4ca5-a9ea-c7c5dff89b1d",
            "name": "load balancer",
            "description": "Forward IPs across multiple outputs using round robin.",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "AnyPointer", "desc": "The IP to forward to one attached outport"},
        ],
        "outPorts": [
            {
                "name": "out",
                "type": "array",
                "contentType": "AnyPointer",
                "desc": "Outgoing IPs distributed one-by-one across attached outports",
            },
        ],
    },
}


class LoadBalancer(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while any(self.array_out_ports["out"]):
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            out_ip = copy_ip(in_ip)
            if not await self.write_array_out("out", process.ArrayOutStrategy.ROUND_ROBIN, out_ip):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(LoadBalancer(meta), meta)


if __name__ == "__main__":
    main()
