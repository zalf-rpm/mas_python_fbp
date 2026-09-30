#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"""Copyable template for a `standard` (Runnable) component.

**Prefer the Process style** for new components in this repository - see
`process_component_template.py`. A Process component gets runtime-owned `conf` and `log` ports,
config applied before it starts and between IPs, lifecycle and activity reporting, array port
strategies, chunked IO, and cooperative stop. None of that is available here, and every component
in the base set is written that way.

The Runnable style is still supported rather than merely tolerated: it is what the C++
implementation currently uses, and Python is so far the only one implementing the `Process`
interface. It is also conceptually simpler - connect ports, loop, write - which makes it a
reasonable choice for a small component or a second language implementation.

What this template shows, and what a Runnable has to do for itself:

- **Bracket transparency.** Forward bracket IPs unchanged, or a substream passing through is
  destroyed. Rebuilding an IP with `fbp_capnp.IP.new_message(content=...)` drops its type, so a
  bracket would come out as a standard IP.
- **Config.** There is no runtime to apply it: read the `conf` port yourself, once, before the loop.
- **Attributes.** `components/common/brackets.py` works here too, and writes values as
  `common.Value` with `valueType` set, which is the convention the rest of the library reads.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import capnp
from mas.schema.fbp import fbp_capnp

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.components.common import brackets
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
        description=(
            "Template runnable component to copy and adapt. Prefer the Process template for new "
            "components; this style exists for parity with implementations that do not offer the "
            "Process interface yet. Substream transparent: bracket IPs are forwarded unchanged."
        ),
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
    # A Runnable applies its own config: there is no runtime doing it before the loop starts.
    if await p.update_config_from_port(config, pc.in_ports["conf"]):
        logger.info("%s: %s updated config from conf port", Path(__file__).name, config["name"])

    while pc.in_ports["in"] and pc.out_ports["out"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)

            # Forward the caller's grouping untouched. Rebuilding the IP below would drop its type,
            # turning a bracket into a standard IP and destroying the substream.
            if brackets.is_bracket(in_ip):
                await pc.out_ports["out"].write(value=in_ip)
                continue

            text = in_ip.content.as_text()
            out_ip = fbp_capnp.IP.new_message(content=f"{config['prefix']}{text}")
            brackets.copy_attrs(in_ip, out_ip, extra=_extra_attributes(config))
            await pc.out_ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            logger.exception("%s: %s RPC Exception: %s", Path(__file__).name, config["name"], e.description)
            if e.type in ["DISCONNECTED"]:
                break

    await pc.close_out_ports()
    logger.info("%s: %s process finished", Path(__file__).name, config["name"])


def _extra_attributes(config: dict[str, Any]) -> dict[str, str]:
    """Example extra attributes, added to the ones copied from the input.

    Plain Python values: `brackets.copy_attrs` wraps them into a `common.Value` with `valueType`
    set, which is what the rest of the library expects to read.
    """

    attribute_name = config.get("attribute_name")
    if not attribute_name:
        return {}
    return {str(attribute_name): str(config["name"])}


def main():
    """Run the template component with the default runnable CLI/bootstrap flow."""

    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
