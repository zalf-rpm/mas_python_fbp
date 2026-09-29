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

JSON_CONTENT_TYPE = "Text (JSON)"


class Config(process.ProcessConfig):
    content_type: str | None = Field(
        None,
        description=(
            "Cap'n Proto type to read the content as, for IPs that carry no "
            "sysAttributes.contentType themselves. An IP's own type always wins."
        ),
    )
    traversal_path: str | None = Field(
        None,
        description="Optional path into the converted structure, emitting that part instead of the whole.",
    )
    path_separator: str = Field("/", description="Separator used for traversal_path.")
    indent: int | None = Field(None, description="Indentation for the emitted JSON. None emits it compact.")
    include_attributes: bool = Field(
        False,
        description=(
            "Emit {'content': ..., 'attributes': {...}} instead of the bare content, so an IP's "
            "attributes survive the conversion into JSON."
        ),
    )
    parse_text_as_json: bool = Field(
        False,
        description=(
            "When the content is plain Text, parse it as JSON instead of emitting it as a JSON "
            "string. Useful downstream of the JSON components here, which emit JSON as untyped "
            "Text and would otherwise be encoded twice. Off by default: a text payload that merely "
            "looks like JSON would change meaning, e.g. the string '123' becoming the number 123."
        ),
    )
    data_as: Literal["base64", "hex", "list"] = Field(
        "base64",
        description="How to encode Cap'n Proto Data fields, which JSON cannot hold directly.",
    )
    unresolved: Literal["drop", "null", "repr"] = Field(
        "drop",
        description=(
            "What to do with pointers carrying no recoverable type, such as an AnyPointer field or "
            "a capability: leave the key out, emit null, or emit a debug representation."
        ),
    )
    on_error: Literal["skip", "pass_through", "fail"] = Field(
        "skip",
        description=(
            "Per IP: skip it, forward the input unchanged, or let the process fail, when the "
            "content cannot be converted."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(id="convert", name="Convert"),
    info=meta.Info(
        id="181b4a79-d1c0-4f19-90b8-26191806ae78",
        name="Cap'n Proto to JSON",
        description=(
            "Convert an IP's Cap'n Proto content into JSON text, using the type the IP declares or "
            "one from config. Substream transparent. The inverse of 'JSON to Cap'n Proto'."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="IPs whose content should be converted. Their content type drives the conversion.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(name="out", contentType=JSON_CONTENT_TYPE, desc="The content as JSON text.", required=True),
        meta.Port(
            name="err",
            contentType="AnyPointer",
            desc="IPs whose content could not be converted, forwarded unchanged. Optional.",
        ),
    ],
    config=Config,
)


class CapnpToJson(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _converted(self, in_ip: Any) -> Any:
        """The IP's content as JSON-compatible Python, or MISSING."""
        content_type = values.content_type_of(in_ip) or self.config.content_type
        schema = values.resolve_schema(content_type)

        if schema is None or getattr(schema, "node", None) is None or schema.node.which() != "struct":
            # No struct schema: text content is still recoverable, anything else is not (D14).
            resolved = values.python_from_any(in_ip.content, content_type)
            if self.config.parse_text_as_json and isinstance(resolved, str):
                try:
                    return json.loads(resolved)
                except (json.JSONDecodeError, ValueError):
                    logger.debug("%s: content is not JSON text; emitting it as a string.", self.name)
            return resolved

        try:
            reader = in_ip.content.as_struct(schema)
        except capnp.KjException:
            logger.debug("%s: content did not read as %r", self.name, content_type, exc_info=True)
            return values.MISSING

        return values.json_from_capnp(
            reader,
            schema,
            data_as=self.config.data_as,
            unresolved=self.config.unresolved,
        )

    def _payload_for(self, in_ip: Any) -> Any:
        converted = self._converted(in_ip)
        if converted is values.MISSING:
            return values.MISSING

        if self.config.traversal_path:
            path = selectors.split_path(self.config.traversal_path, self.config.path_separator)
            converted = selectors.apply_path(converted, path)
            if converted is values.MISSING:
                logger.warning(
                    "%s: could not resolve traversal_path %r in the converted content.",
                    self.name,
                    self.config.traversal_path,
                )
                return values.MISSING

        if not self.config.include_attributes:
            return converted

        attributes = {
            name: (None if value is values.MISSING else value) for name, value in brackets.attrs_as_dict(in_ip).items()
        }
        return {"content": converted, "attributes": attributes}

    async def _report_failure(self, in_ip: Any) -> bool:
        """Deal with an IP that could not be converted. Returns False to end the run."""
        if self.config.on_error == "fail":
            msg = f"{self.name} could not convert content of type {values.content_type_of(in_ip) or 'unset'}"
            raise ValueError(msg)
        if self.out_ports["err"] is not None:
            return await self.write_out("err", in_ip)
        if self.config.on_error == "pass_through":
            return await self.write_out("out", in_ip)
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            payload = self._payload_for(in_ip)
            if payload is values.MISSING:
                if not await self._report_failure(in_ip):
                    break
                continue

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(payload, indent=self.config.indent, default=str))
            out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE
            brackets.copy_attrs(in_ip, out_ip)
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(CapnpToJson(METADATA), METADATA)


if __name__ == "__main__":
    main()
