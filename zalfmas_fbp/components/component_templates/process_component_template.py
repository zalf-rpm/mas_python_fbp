#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"""Copyable template for a Process-based component.

Replace the placeholder metadata values, then adapt the typed config, ports,
and `run()` logic. The example keeps incoming attributes and shows how to add
one more attribute to the outgoing IP.
"""

from __future__ import annotations

import logging
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class TemplateProcessConfig(process.ProcessConfig):
    """Example typed config model used as the single source of truth for metadata."""

    prefix: str = Field("", description="Optional prefix added to each outgoing message.")
    attribute_name: str | None = Field(
        "processedBy",
        description="If set, add this attribute to outgoing IPs with the component name as value.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="templates",
        name="Templates",
    ),
    info=meta.Info(
        id="replace-with-process-component-id",
        name="replace with process component name",
        description="Template process component to copy and adapt.",
    ),
    type="process",
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
    config=TemplateProcessConfig,
)


class TemplateProcessComponent(process.Process[TemplateProcessConfig]):
    """Minimal Process component showing config, IO, and attribute propagation."""

    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        """Read input IPs, transform content, preserve attrs, and emit output IPs."""

        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            text = in_msg.content.as_text()
            out_ip = fbp_capnp.IP.new_message(content=f"{self.config.prefix}{text}")
            common.copy_and_set_fbp_attrs(in_msg, out_ip, **self._extra_attributes())
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)

    def _extra_attributes(self) -> dict[str, str]:
        """Return example extra attributes to attach in addition to copied input attrs."""

        if self.config.attribute_name is None:
            return {}
        return {self.config.attribute_name: self.name}


def main():
    """Run the template component with the default Process CLI/bootstrap flow."""

    process.run_process_from_metadata_and_cmd_args(TemplateProcessComponent(METADATA), METADATA)


if __name__ == "__main__":
    main()
