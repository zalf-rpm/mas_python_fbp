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
from typing import Any

import capnp
from capnp import types as capnp_types
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class ToStringConfig(process.ProcessConfig):
    struct_type: str | None = Field(
        "@0xed6c098b67cad454 = common/common.capnp:StructuredText",
        description=(
            "A Cap'n Proto content type string with a struct schema id (e.g. "
            "'@0xed6c098b67cad454 = common/common.capnp:StructuredText') or 'Text' for "
            "plain text content. If the incoming IP has a parseable sysAttributes.contentType, "
            "that schema takes precedence over this configured value."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="string",
        name="String",
    ),
    info=meta.Info(
        id="250488d8-7519-49a8-820e-0e981ffb2a71",
        name="to string",
        description="Outputs input structures as string (if possible).",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyStruct",
        ),
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
        ),
    ],
    config=ToStringConfig,
)


def _schema_from_content_type_string(content_type: str | None) -> Any | None:
    if not content_type:
        return None

    try:
        return common.schema_from_content_type_string(content_type)
    except (AttributeError, RuntimeError, TypeError, ValueError, capnp.KjException):
        logger.debug("Failed to parse Cap'n Proto schema from content type %r.", content_type, exc_info=True)
        return None


def _content_to_str(c: Any, schema_or_type: Any) -> str:
    if hasattr(schema_or_type, "as_struct"):
        c = c.as_struct(schema_or_type.as_struct())
    elif schema_or_type is capnp_types.Text:
        return c.as_text()
    elif schema_or_type is capnp_types.Void:
        return ""
    elif schema_or_type is capnp_types.AnyPointer:
        return str(c)

    return str(c)


class ToString(process.Process[ToStringConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        configured_schema = _schema_from_content_type_string(self.config.struct_type)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            c = in_msg.content
            resolved = _schema_from_content_type_string(process.ip_content_type(in_msg))
            if resolved is capnp_types.AnyPointer or resolved is None:
                resolved = configured_schema

            if resolved is None or resolved is capnp_types.AnyPointer:
                c_str = str(c)
            else:
                c_str = _content_to_str(c, resolved)

            logger.info("%s received: %s", self.name, c_str)

            out_ip = fbp_capnp.IP.new_message(content=c_str)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return
            logger.info("%s sent: %s", self.name, c_str)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(ToString(METADATA), METADATA)


if __name__ == "__main__":
    main()
