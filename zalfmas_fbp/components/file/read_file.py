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
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

METADATA = meta.Component(
    category=meta.Category(
        id="file",
        name="File",
    ),
    info=meta.Info(
        id="7ba769ca-eba1-437c-b61a-bef27e24b1dc",
        name="read file",
        description="Read a file and send full string or lines downstream.",
    ),
    type="standard",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="Output either full file content or each line as as separate message.",
        ),
    ],
    defaultConfig={
        "to_attr": meta.ConfigEntry(
            value=None,
            type="string",
            desc="store read file content into 'to_attr'",
        ),
        "file": meta.ConfigEntry(
            value="",
            type="string",
            desc="Path to file to read.",
        ),
        "lines_mode": meta.ConfigEntry(
            value=True,
            type="bool",
            desc="Send single lines if true else send whole file content at once.",
        ),
        "skip_lines": meta.ConfigEntry(
            value=0,
            type="int",
            desc="If lines mode is true, skip that many lines at the beginning of the file.",
        ),
    },
)


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf"], outs=["out"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    skip_lines = config["skip_lines"]
    if config["file"] and pc.out_ports["out"]:
        try:
            with open(config["file"]) as _:
                if config["lines_mode"]:
                    for line in _:
                        if skip_lines > 0:
                            skip_lines -= 1
                            continue

                        out_ip = fbp_capnp.IP.new_message()
                        if config["to_attr"] and len(config["to_attr"]) > 0:
                            out_ip.attributes = [{"key": config["to_attr"], "value": line}]  # pyright: ignore
                        else:
                            out_ip.content = line
                        await pc.out_ports["out"].write(value=out_ip)
                else:
                    file_content = _.read()
                    out_ip = fbp_capnp.IP.new_message()
                    if config["to_attr"] and len(config["to_attr"]) > 0:
                        out_ip.attributes = [{"key": config["to_attr"], "value": file_content}]  # pyright: ignore
                    else:
                        out_ip.content = file_content
                    await pc.out_ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            logger.error("%s: RPC Exception: %s", os.path.basename(__file__), e.description)

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
