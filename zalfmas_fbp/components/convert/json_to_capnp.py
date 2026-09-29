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

import json
import logging
from typing import Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    content_type: str = Field(
        "",
        description=(
            "Cap'n Proto type to build, e.g. "
            "'@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup'. Required unless a type "
            "arrives on the 'type' port."
        ),
    )
    traversal_path: str | None = Field(
        None,
        description="Optional path into the incoming JSON, building from that part instead of the whole.",
    )
    path_separator: str = Field("/", description="Separator used for traversal_path.")
    unknown_fields: Literal["error", "ignore"] = Field(
        "error",
        description="Whether a JSON key the target type does not have is an error or is dropped.",
    )
    coerce_numbers: bool = Field(
        True,
        description="Accept '5' for an integer field, an int for a float field, and base64 text for Data.",
    )
    on_error: Literal["skip", "pass_through", "fail"] = Field(
        "skip",
        description="Per IP: skip it, forward the input unchanged, or let the process fail.",
    )


METADATA = meta.Component(
    category=meta.Category(id="convert", name="Convert"),
    info=meta.Info(
        id="43d107ff-c1b5-4622-a050-3445b8f7c159",
        name="JSON to Cap'n Proto",
        description=(
            "Build a typed Cap'n Proto struct from JSON text, tagging the outgoing IP with the "
            'type it built. Substream transparent. The inverse of "Cap\'n Proto to JSON".'
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType="Text (JSON)", desc="JSON text to build from.", required=True),
        meta.Port(
            name="type",
            contentType="Text",
            desc=(
                "Optional. A Cap'n Proto content-type string, read once at start, overriding the "
                "configured 'content_type' - so the target type can come from the flow."
            ),
            role="control",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The built struct, with sysAttributes.contentType set to the type that was built.",
            required=True,
        ),
        meta.Port(
            name="err",
            contentType="Text (JSON)",
            desc="The original JSON for IPs that could not be built. Optional.",
        ),
    ],
    config=Config,
)


class JsonToCapnp(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._content_type: str = ""

    async def _resolve_target_type(self) -> Any | None:
        """The schema to build, from the 'type' port if connected, else from config."""
        content_type = self.config.content_type
        if self.in_ports["type"] is not None:
            type_ip = await self.read_in("type")
            if type_ip is not None:
                try:
                    content_type = type_ip.content.as_text()
                except capnp.KjException:
                    logger.warning("%s: 'type' port carried no text; using the configured type.", self.name)

        if not content_type:
            logger.error("%s needs a 'content_type', by config or on the 'type' port.", self.name)
            return None

        schema = values.resolve_schema(content_type)
        if schema is None:
            logger.error("%s could not resolve the target type %r.", self.name, content_type)
            return None

        self._content_type = content_type
        return schema

    def _built(self, in_ip: Any, schema: Any) -> Any:
        try:
            payload = json.loads(in_ip.content.as_text())
        except (capnp.KjException, json.JSONDecodeError, UnicodeDecodeError, ValueError):
            logger.warning("%s: input was not readable JSON text.", self.name)
            return values.MISSING

        if self.config.traversal_path:
            path = selectors.split_path(self.config.traversal_path, self.config.path_separator)
            payload = selectors.apply_path(payload, path)
            if payload is values.MISSING:
                logger.warning("%s: could not resolve traversal_path %r.", self.name, self.config.traversal_path)
                return values.MISSING

        try:
            return values.capnp_from_json(
                payload,
                schema,
                unknown_fields=self.config.unknown_fields,
                coerce_numbers=self.config.coerce_numbers,
            )
        except (TypeError, ValueError) as exc:
            logger.warning("%s could not build %s: %s", self.name, self._content_type, exc)
            return values.MISSING

    async def _report_failure(self, in_ip: Any) -> bool:
        if self.config.on_error == "fail":
            msg = f"{self.name} could not build {self._content_type} from the incoming JSON"
            raise ValueError(msg)
        if self.out_ports["err"] is not None:
            return await self.write_out("err", in_ip)
        if self.config.on_error == "pass_through":
            return await self.write_out("out", in_ip)
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        schema = await self._resolve_target_type()
        if schema is None:
            logger.info("%s process finished", self.name)
            return

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            built = self._built(in_ip, schema)
            if built is values.MISSING:
                if not await self._report_failure(in_ip):
                    break
                continue

            out_ip = fbp_capnp.IP.new_message(content=built)
            out_ip.sysAttributes.contentType = self._content_type
            brackets.copy_attrs(in_ip, out_ip)
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(JsonToCapnp(METADATA), METADATA)


if __name__ == "__main__":
    main()
