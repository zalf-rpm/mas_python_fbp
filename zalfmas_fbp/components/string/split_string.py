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
from pathlib import Path
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

METADATA = meta.Component(
    category=meta.Category(
        id="string",
        name="String",
    ),
    info=meta.Info(
        id="d5c2fc62-2be0-4a25-aafe-e710ac3fb39c",
        name="split string",
        description="Splits a string along delimiter.",
    ),
    type="standard",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
        ),
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
        ),
    ],
    defaultConfig={
        "split_at": meta.ConfigEntry(
            value=",",
            type="string",
            desc="Split string at this character.",
        ),
    },
)


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    logger.info("%s: %s connected port(s)", Path(__file__).name, config["name"])
    _ = await p.update_config_from_port(config, pc.in_ports["conf"])
    if pc.in_ports["conf"]:
        logger.info("%s: %s updated config from config port", Path(__file__).name, config["name"])

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            s: str = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
            logger.info("%s: %s received: %s", Path(__file__).name, config["name"], s)
            s = s.rstrip()
            vals = s.split(config["split_at"])

            for val in vals:
                out_ip = fbp_capnp.IP.new_message(content=val)
                await pc.out_ports["out"].write(value=out_ip)
                logger.info("%s: %s sent: %s", Path(__file__).name, config["name"], val)

        except capnp.KjException as e:
            logger.exception("%s: %s RPC Exception: %s", Path(__file__).name, config["name"], e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: %s process finished", Path(__file__).name, config["name"])


def main():
    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
