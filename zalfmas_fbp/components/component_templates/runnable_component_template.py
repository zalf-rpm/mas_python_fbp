#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"""Copyable template for a standard runnable component.

Replace the placeholder metadata values, then adapt the ports and coroutine
body. The example keeps incoming attributes and shows how to add one more
attribute to the outgoing IP.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

METADATA = meta.Component(
    category=meta.Category(
        id="templates",
        name="Templates",
    ),
    info=meta.Info(
        id="replace-with-runnable-component-id",
        name="replace with runnable component name",
        description="Template runnable component to copy and adapt.",
    ),
    type="standard",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
            desc="Incoming text messages.",
        ),
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
            desc="Optional runtime configuration updates.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="Outgoing text messages.",
        ),
    ],
    defaultConfig={
        "prefix": meta.ConfigEntry(
            value="",
            type="string",
            desc="Optional prefix added to each outgoing message.",
        ),
        "attribute_name": meta.ConfigEntry(
            value="processedBy",
            type="string",
            desc="If set, add this attribute to outgoing IPs with the component name as value.",
        ),
    },
)


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
    """Connect ports, process incoming IPs, and emit updated output IPs."""

    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"], outs=["out"])
    logger.info("%s: %s connected port(s)", Path(__file__).name, config["name"])
    _ = await p.update_config_from_port(config, pc.in_ports["conf"])
    if pc.in_ports["conf"]:
        logger.info("%s: %s updated config from conf port", Path(__file__).name, config["name"])

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            text = in_ip.content.as_text()
            out_ip = fbp_capnp.IP.new_message(content=f'{config["prefix"]}{text}')
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **_extra_attributes(config))
            await pc.out_ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            logger.exception("%s: %s RPC Exception: %s", Path(__file__).name, config["name"], e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: %s process finished", Path(__file__).name, config["name"])


def _extra_attributes(config: dict[str, Any]) -> dict[str, str]:
    """Return example extra attributes to attach in addition to copied input attrs."""

    attribute_name = config.get("attribute_name")
    if not attribute_name:
        return {}
    return {str(attribute_name): str(config["name"])}


def main():
    """Run the template component with the default runnable CLI/bootstrap flow."""

    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
