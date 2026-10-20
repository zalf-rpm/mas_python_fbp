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

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

JSON_CONTENT_TYPE = "Text (JSON)"


class Config(process.ProcessConfig):
    strategy: Literal["deep", "shallow", "replace"] = Field(
        default="deep",
        description=(
            "'deep' merges nested objects key by key, 'shallow' replaces a whole value at the top "
            "level, 'replace' takes the patch entirely."
        ),
    )
    list_strategy: Literal["replace", "append", "by_key"] = Field(
        default="replace",
        description=(
            "How lists combine: the patch's list wins, the two are concatenated, or items are "
            "matched by 'list_key' and merged."
        ),
    )
    list_key: str = Field(default="id", description="The item key used by the 'by_key' list strategy.")
    null_deletes: bool = Field(
        default=False,
        description="Treat a null in the patch as 'remove this key' rather than as the value null.",
    )
    patch_wins: bool = Field(
        default=True,
        description="On a conflict, take the patch's value. Off keeps the base's.",
    )


METADATA = meta.Component(
    category=meta.Category(id="json", name="JSON"),
    info=meta.Info(
        id="807c3185-83fc-4a4f-8a4e-38b58970b711",
        name="Merge JSON",
        description=(
            "Merge a patch received on 'patch' into a base received on 'in'. Where 'Update JSON' "
            "patches from config or attributes, this patches from a second stream."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType=JSON_CONTENT_TYPE, desc="The base documents.", required=True),
        meta.Port(
            name="patch",
            contentType=JSON_CONTENT_TYPE,
            desc=(
                "The documents to merge in, one per base. A patch read once and reused for every "
                "base is not supported; connect 'Copy IP' if that is wanted."
            ),
            required=True,
        ),
    ],
    outPorts=[meta.Port(name="out", contentType=JSON_CONTENT_TYPE, desc="The merged documents.", required=True)],
    config=Config,
)


def merge(base: Any, patch: Any, config: Config) -> Any:
    """Merge patch into base per the configured strategy."""
    if config.strategy == "replace":
        return patch
    if not isinstance(base, dict) or not isinstance(patch, dict):
        return _merge_non_objects(base, patch, config)

    merged = dict(base)
    for key, value in patch.items():
        if value is None and config.null_deletes:
            _ = merged.pop(key, None)
            continue
        if key not in merged:
            merged[key] = value
            continue
        if not config.patch_wins and config.strategy != "deep":
            continue
        if config.strategy == "deep":
            merged[key] = merge(merged[key], value, config)
        else:
            merged[key] = value if config.patch_wins else merged[key]
    return merged


def _merge_non_objects(base: Any, patch: Any, config: Config) -> Any:
    if isinstance(base, list) and isinstance(patch, list):
        if config.list_strategy == "append":
            return [*base, *patch]
        if config.list_strategy == "by_key":
            by_key = {item.get(config.list_key): item for item in base if isinstance(item, dict)}
            merged = list(base)
            for item in patch:
                key = item.get(config.list_key) if isinstance(item, dict) else None
                if key is not None and key in by_key:
                    merged[merged.index(by_key[key])] = merge(by_key[key], item, config)
                else:
                    merged.append(item)
            return merged
        return patch
    return patch if config.patch_wins else base


class MergeJson(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _document(self, in_ip: Any, label: str) -> Any:
        try:
            return json.loads(in_ip.content.as_text())
        except (capnp.KjException, json.JSONDecodeError, UnicodeDecodeError, ValueError):
            logger.warning("%s: %s was not readable JSON text.", self.name, label)
            return None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        merged_count = 0
        while self.in_ports["in"] and self.in_ports["patch"] and self.out_ports["out"]:
            base_ip = await self.read_in("in")
            if base_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(base_ip):
                if not await self.write_out("out", base_ip):
                    break
                continue

            patch_ip = await self.read_in("patch")
            if patch_ip is None:
                self.in_ports["patch"] = None
                logger.info("%s: 'patch' closed; forwarding the remaining base unchanged.", self.name)
                _ = await self.write_out("out", base_ip)
                break

            base = self._document(base_ip, "base")
            patch = self._document(patch_ip, "patch")
            if base is None or patch is None:
                if not await self.write_out("out", base_ip):
                    break
                continue

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(merge(base, patch, self.config), default=str))
            out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE
            brackets.copy_attrs(base_ip, out_ip, extra=brackets.attr_readers(patch_ip))

            merged_count += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, merged %d document(s)", self.name, merged_count)


def main():
    process.run_process_from_metadata_and_cmd_args(MergeJson(METADATA), METADATA)


if __name__ == "__main__":
    main()
