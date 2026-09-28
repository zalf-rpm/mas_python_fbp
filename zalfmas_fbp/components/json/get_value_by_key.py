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
from typing import Any, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field, JsonValue
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

_PARSE_FAILED = object()  # content wasn't usable JSON, keep the current object
_MISSING = object()  # path (or single key) did not resolve to anything


class Config(process.ProcessConfig):
    path_separator: str = Field(
        "/",
        description=(
            "Separator used to split a 'key' input into a path of nested keys/list indices (e.g. "
            "'a/b/0/c' to reach obj['a']['b'][0]['c']), so subobjects can be accessed - same "
            "convention as Filter JSON's traversal_path/path_separator. Set to an empty string to "
            "disable splitting and treat the whole decoded key as a single, top-level key."
        ),
    )
    emit_message_for_missing_key: bool = Field(
        True,
        description=(
            "Whether to write a message on 'value' at all when the requested key/path is missing from "
            "the current object (or no object has been received yet). If false, that 'key' input is "
            "silently dropped without any output on 'value'."
        ),
    )
    missing_key_value: JsonValue = Field(
        "",
        description=(
            "JSON value serialized onto 'value' when the requested key/path is missing, only used if "
            "emit_message_for_missing_key is true. Defaults to an empty JSON string; set to null, an "
            "object, etc. as needed - JSON null ('null') is a perfectly valid standalone JSON document."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="a45375ea-a7a9-4835-b165-2460f306dfc0",
        name="Get value by key",
        description=(
            "Look up a key or path (received on 'key') in a JSON object (received on 'obj'), like a "
            "Python dict.get() into possibly nested subobjects, and write the serialized result on "
            "'value'."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="obj",
            contentType="Text (JSON)",
            desc=(
                "The JSON object to look keys up in. The first message received is the initial object; "
                "if this port then closes, that object is kept in memory for good. While the port stays "
                "open, each 'key' input triggers a single non-blocking check (readIfMsg) for a newer "
                "object, which replaces the stored one if available, or is skipped otherwise. A substream "
                "on this port is drained and treated as one single incoming object update, same as an "
                "unwrapped message."
            ),
        ),
        meta.Port(
            name="key",
            contentType="Text (JSON)",
            desc=(
                "JSON-encoded key (or 'path_separator'-delimited path, e.g. 'a/b/0/c') to look up in "
                "the current object, one lookup per message received. Bracket IPs are forwarded "
                "unchanged onto 'value'; all 'key' IPs inside one substream look up against the same "
                "single object snapshot, taken (subject to the same readIfMsg/substream rules as an "
                "unwrapped 'key' message) when the substream's open-bracket arrives."
            ),
        ),
    ],
    outPorts=[
        meta.Port(
            name="value",
            contentType="Text (JSON)",
            desc="JSON-encoded value found for the requested key/path, or the configured missing-key value.",
        ),
    ],
    config=Config,
)


def _parse_json(content: str, port_label: str, process_name: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        logger.warning("%s received invalid JSON on '%s': %s", process_name, port_label, exc)
        return _PARSE_FAILED


def _split_key_path(key: Any, separator: str) -> list[Any]:
    if not isinstance(key, str) or separator == "":
        return [key]

    parts: list[str | int] = []
    for part in key.split(separator):
        if part == "":
            continue
        if part.lstrip("-").isdigit():
            parts.append(int(part))
        else:
            parts.append(part)
    return parts


def _resolve_path(value: Any, parts: list[Any]) -> Any:
    current = value
    for part in parts:
        if isinstance(part, int) and not isinstance(part, bool):
            if isinstance(current, list) and -len(current) <= part < len(current):
                current = current[part]
                continue
            return _MISSING

        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return _MISSING
    return current


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._obj: Any = None

    async def _drain_obj_substream(self) -> Any:
        """Read (blocking) until the matching close-bracket, merging any JSON object payloads found
        inside via dict.update() (later keys override earlier ones). Assumes the opening open-bracket
        on 'obj' has already been consumed by the caller.
        """
        nesting_level = 1
        merged: dict[str, Any] | None = None
        while nesting_level > 0:
            ip = await self.read_in("obj")
            if ip is None:
                logger.warning(
                    "%s: 'obj' port closed mid-substream; using whatever was collected so far.",
                    self.name,
                )
                break
            if ip.type == "openBracket":
                nesting_level += 1
                continue
            if ip.type == "closeBracket":
                nesting_level -= 1
                continue
            parsed = _parse_json(ip.content.as_text(), "obj", self.name)
            if parsed is _PARSE_FAILED:
                continue
            if isinstance(parsed, dict):
                merged = {**(merged or {}), **parsed}
            else:
                logger.warning("%s: ignoring non-object JSON value inside 'obj' substream.", self.name)
        return _PARSE_FAILED if merged is None else merged

    async def _read_initial_obj(self) -> None:
        if self.in_ports["obj"] is None:
            return

        ip = await self.read_in("obj")
        if ip is None:
            return

        new_obj = await self._drain_obj_substream() if ip.type == "openBracket" else _parse_json(
            ip.content.as_text(), "obj", self.name
        )
        if new_obj is not _PARSE_FAILED:
            self._obj = new_obj

    async def _maybe_refresh_obj(self) -> None:
        """Non-blocking best-effort check for a newer object; leaves the stored object untouched
        if none is available yet, and permanently stops checking once the port reports done.
        """
        port = self.in_ports["obj"]
        if port is None:
            return

        try:
            msg = await port.readIfMsg()
        except capnp.KjException:
            logger.exception("%s: readIfMsg on 'obj' failed", self.name)
            return

        which = msg.which()
        if which == "noMsg":
            return
        if which == "done":
            self.in_ports["obj"] = None
            return

        ip = msg.value.as_struct(fbp_capnp.IP)
        new_obj = await self._drain_obj_substream() if ip.type == "openBracket" else _parse_json(
            ip.content.as_text(), "obj", self.name
        )
        if new_obj is not _PARSE_FAILED:
            self._obj = new_obj

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        await self._read_initial_obj()

        # depth > 0 means we're inside a 'key' substream: the object snapshot is taken once, when
        # the outermost open-bracket arrives, and every 'key' IP inside looks up against that same
        # snapshot - only once depth returns to 0 does a plain (or the next substream's) 'key' IP
        # trigger another check.
        key_substream_depth = 0

        while True:
            key_ip = await self.read_in("key")
            if key_ip is None:
                break

            if key_ip.type == "openBracket":
                if key_substream_depth == 0:
                    await self._maybe_refresh_obj()
                key_substream_depth += 1
                if not await self.write_out("value", key_ip):
                    logger.info("%s process finished", self.name)
                    return
                continue

            if key_ip.type == "closeBracket":
                key_substream_depth = max(0, key_substream_depth - 1)
                if not await self.write_out("value", key_ip):
                    logger.info("%s process finished", self.name)
                    return
                continue

            if key_substream_depth == 0:
                await self._maybe_refresh_obj()

            key = _parse_json(key_ip.content.as_text(), "key", self.name)
            if key is _PARSE_FAILED:
                continue

            value = _resolve_path(self._obj, _split_key_path(key, self.config.path_separator))
            if value is _MISSING:
                if not self.config.emit_message_for_missing_key:
                    continue
                value = self.config.missing_key_value

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(value), attributes=key_ip.attributes)
            if not await self.write_out("value", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
