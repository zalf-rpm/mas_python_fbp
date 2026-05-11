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

import logging
import os
import sys
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

METADATA = meta.Component(
    category=meta.Category(
        id="console",
        name="Console",
    ),
    info=meta.Info(
        id="2de9c491-d8a6-4b36-84de-db7f4a312731",
        name="output to console",
        description="Output input to console.",
    ),
    type="standard",
    inPorts=[
        meta.Port(
            name="in",
        ),
    ],
)


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["in"])
    logger.info("%s: connected port(s)", os.path.basename(__file__))

    while pc.in_ports["in"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            try:
                sys.stdout.write(f"{in_ip.content.as_text()}\n")
            except (capnp.KjException, TypeError):
                sys.stdout.write(f"{in_ip.content}\n")
            sys.stdout.flush()

        except capnp.KjException as e:
            logger.error("%s: RPC Exception: %s", os.path.basename(__file__), e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
