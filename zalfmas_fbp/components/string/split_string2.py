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

import asyncio
import os

import capnp
from run import process
from zalfmas_capnp_schemas_with_stubs import common_capnp, fbp_capnp
from zalfmas_common import common


class SplitString(process.Process):

    def __init__(self, con_man: common.ConnectionManager = None):
        process.Process.__init__(self, con_man)
        self.name = "split string"
        self.description="Split a string."
        self.in_ports_config = {"in": "text"}
        self.out_ports_config = {"in": "text"}
        self.config["split_at"] = common_capnp.Value.new_message(t=",")

    @property
    def IN(self):
        return self.in_ports["in"]

    @IN.setter
    def IN(self, value):
        self.in_ports["in"] = value

    @property
    def OUT(self):
        return self.out_ports["out"]

    async def start(self):
        while self.IN and self.OUT:
            try:
                in_msg = await self.IN.read()
                if in_msg.which() == "done":
                    self.IN = None
                    continue

                s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                print(f"{os.path.basename(__file__)}: {self.name} received:", s)
                s = s.rstrip()
                vals = s.split(self.config["split_at"])

                for val in vals:
                    out_ip = fbp_capnp.IP.new_message(content=val)
                    await self.OUT.write(value=out_ip)
                    print(f"{os.path.basename(__file__)}: {self.name} sent:", val)

            except capnp.KjException as e:
                print(
                    f"{os.path.basename(__file__)}: {self.name} RPC Exception:",
                    e.description,
                )
                if e.type in ["DISCONNECTED"]:
                    break

        print(f"{os.path.basename(__file__)}: {self.name} process finished")


def main():
    parser, args = process.create_default_args_parser(component_description="split string")
    asyncio.run(capnp.run(SplitString().serve(args.writer_sr)))

if __name__ == "__main__":
    main()