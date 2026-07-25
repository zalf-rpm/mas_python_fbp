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
from typing import Any

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

_MISSING = object()


class FilterJsonConfig(process.ProcessConfig):
    traversal_path: str | None = Field(
        None,
        description=(
            "Optional path (e.g. 'obs_crop_daily_means' or 'root/items/0') from the input JSON root "
            "to the leaf structure that should be filtered."
        ),
    )
    path_separator: str = Field(
        "/",
        description="Separator used for traversal_path and filter_paths.",
    )
    filter_paths: list[str] = Field(
        default_factory=list,
        description=(
            "List of paths to keep from each filtered item. Supports nested paths and list indices "
            "(e.g. ['DATE', 'stats/CWAD', '0/value']). Optional aliases can be provided as "
            "'alias=path' (e.g. 'date=DATE')."
        ),
    )
    values_only: bool = Field(
        False,
        description=(
            "If true, return only values in the order of filter_paths instead of key/value objects. "
            "For a list leaf this results in a list of lists."
        ),
    )
    flatten_values_only_lists: bool = Field(
        False,
        description=(
            "If true and values_only is true, flatten one list nesting level for list outputs. "
            "This turns list-of-lists into a single list."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="161c9d39-a3c5-4885-beb2-9d85a7c777bf",
        name="Filter JSON",
        description="Traverse JSON to a leaf and filter it by selected paths.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="Input JSON text to traverse and filter.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="Filtered JSON text.",
        ),
    ],
    config=FilterJsonConfig,
)


def _split_path(path: str, separator: str) -> list[str | int]:
    if separator == "":
        return [path]

    parts: list[str | int] = []
    for part in path.split(separator):
        if part == "":
            continue
        if part.lstrip("-").isdigit():
            parts.append(int(part))
        else:
            parts.append(part)
    return parts


def _resolve_path(value: Any, parts: list[str | int]) -> Any:
    current = value
    for part in parts:
        if isinstance(part, int):
            if isinstance(current, list) and -len(current) <= part < len(current):
                current = current[part]
                continue
            return _MISSING

        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return _MISSING
    return current


def _parse_filter_path(entry: str, separator: str) -> tuple[str, list[str | int]]:
    alias: str | None = None
    path = entry
    if "=" in entry:
        alias, path = entry.split("=", 1)
        alias = alias.strip() or None
        path = path.strip()

    parts = _split_path(path, separator)
    if alias is None:
        alias = str(parts[-1]) if len(parts) > 0 else "value"
    return alias, parts


def _project_item(value: Any, filters: list[tuple[str, list[str | int]]], values_only: bool) -> Any:
    if len(filters) == 0:
        return value

    if values_only:
        out_values: list[Any] = []
        for _, parts in filters:
            selected = _resolve_path(value, parts)
            if selected is not _MISSING:
                out_values.append(selected)
        return out_values

    out_dict: dict[str, Any] = {}
    for alias, parts in filters:
        selected = _resolve_path(value, parts)
        if selected is not _MISSING:
            out_dict[alias] = selected
    return out_dict


def _should_apply_to_object_values(value: dict[str, Any]) -> bool:
    return any(isinstance(v, (dict, list)) for v in value.values())


def _apply_filter(
    value: Any,
    filters: list[tuple[str, list[str | int]]],
    values_only: bool,
    flatten_values_only_lists: bool,
) -> Any:
    if len(filters) == 0:
        return value

    if isinstance(value, list):
        mapped = [_project_item(item, filters, values_only) for item in value]
        if values_only and flatten_values_only_lists:
            flattened: list[Any] = []
            for entry in mapped:
                if isinstance(entry, list):
                    flattened.extend(entry)
                else:
                    flattened.append(entry)
            return flattened
        return mapped

    if isinstance(value, dict):
        if _should_apply_to_object_values(value):
            return {
                k: _apply_filter(v, filters, values_only, flatten_values_only_lists)
                for k, v in value.items()
            }
        return _project_item(value, filters, values_only)

    return value


class FilterJson(process.Process[FilterJsonConfig]):
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

        parsed_filters = [_parse_filter_path(f, self.config.path_separator) for f in self.config.filter_paths]

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            if in_msg.type in ("openBracket", "closeBracket"):
                if not await self.write_out("out", in_msg):
                    logger.info("%s process finished", self.name)
                    return
                continue

            try:
                input_json = json.loads(in_msg.content.as_text())
            except json.JSONDecodeError as exc:
                logger.warning("%s received invalid JSON input: %s", self.name, exc)
                continue

            try:
                leaf = (
                    _resolve_path(input_json, _split_path(self.config.traversal_path, self.config.path_separator))
                    if self.config.traversal_path
                    else input_json
                )
                if leaf is _MISSING:
                    logger.warning(
                        "%s could not resolve traversal path '%s'. Sending null.",
                        self.name,
                        self.config.traversal_path,
                    )
                    filtered_json = None
                else:
                    filtered_json = _apply_filter(
                        leaf,
                        parsed_filters,
                        self.config.values_only,
                        self.config.flatten_values_only_lists,
                    )
            except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
                logger.warning("%s failed to filter input JSON: %s", self.name, exc)
                filtered_json = None

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(filtered_json), attributes=in_msg.attributes)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(FilterJson(METADATA), METADATA)


if __name__ == "__main__":
    main()
