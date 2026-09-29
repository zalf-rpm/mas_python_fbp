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
from typing import TYPE_CHECKING, Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

JSON_CONTENT_TYPE = "Text (JSON)"


class Config(process.ProcessConfig):
    traversal_path: str | None = Field(
        None,
        description="Optional path from the document root to the list or object to split.",
    )
    path_separator: str = Field("/", description="Separator used for traversal_path and copy_parent_paths.")
    mode: Literal["list_items", "object_values", "object_entries"] = Field(
        "list_items",
        description=(
            "What to emit: each item of a list, each value of an object, or each of its "
            "{'key': ..., 'value': ...} entries."
        ),
    )
    wrap_in_substream: bool = Field(
        True,
        description="Wrap each document's emitted items in an open-/close-bracket pair.",
    )
    key_attr: str | None = Field(
        None,
        description="For the object modes, attach each entry's key as this attribute.",
    )
    index_attr: str | None = Field(
        None,
        description="Attach each item's 0-based position within its document as this attribute.",
    )
    count_attr: str | None = Field(
        default="substream_length",
        description=(
            "Attach the number of emitted items to the close-bracket as this attribute, the way "
            "'Split bracketed stream' does. Only used when wrapping in a substream. Empty disables "
            "it - a null in a config means 'use the default', so it cannot turn this off."
        ),
    )
    copy_parent_paths: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Attribute name -> path in the *parent* document, copied onto every emitted item. The "
            "usual 'keep the header fields with each row' case."
        ),
    )
    on_error: Literal["skip", "pass_through"] = Field(
        "skip",
        description="What to do with an IP whose content is not readable JSON.",
    )


METADATA = meta.Component(
    category=meta.Category(id="json", name="JSON"),
    info=meta.Info(
        id="205dfc0a-7283-4322-a1b7-4f952abffb88",
        name="Split JSON",
        description=(
            "Explode a JSON list or object into a stream of IPs, one per item, optionally wrapped "
            "in a substream. The inverse of 'Concat JSON substream'."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType=JSON_CONTENT_TYPE, desc="JSON documents to split.", required=True),
    ],
    outPorts=[
        meta.Port(name="out", contentType=JSON_CONTENT_TYPE, desc="One IP per item.", required=True),
    ],
    config=Config,
)


class SplitJson(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _items_of(self, document: Any) -> list[tuple[Any, Any]] | None:
        """(key, value) pairs to emit, or None when the document is not splittable."""
        if self.config.mode == "list_items":
            if not isinstance(document, list):
                return None
            return [(index, item) for index, item in enumerate(document)]

        if not isinstance(document, dict):
            return None
        if self.config.mode == "object_values":
            return list(document.items())
        return [(key, {"key": key, "value": value}) for key, value in document.items()]

    def _parent_attrs(self, document: Any) -> dict[str, Any]:
        inherited: dict[str, Any] = {}
        for name, path in self.config.copy_parent_paths.items():
            resolved = selectors.apply_path(document, selectors.split_path(path, self.config.path_separator))
            if resolved is values.MISSING:
                logger.debug("%s: parent path %r did not resolve; not copying %r.", self.name, path, name)
                continue
            inherited[name] = resolved
        return inherited

    def _item_ip(self, key: Any, value: Any, index: int, in_ip: IPReader, inherited: dict[str, Any]) -> Any:
        out_ip = fbp_capnp.IP.new_message(content=json.dumps(value, default=str))
        out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE

        extra: dict[str, Any] = dict(inherited)
        if self.config.key_attr and self.config.mode != "list_items":
            extra[self.config.key_attr] = key
        if self.config.index_attr:
            extra[self.config.index_attr] = index
        brackets.copy_attrs(in_ip, out_ip, extra=extra)
        return out_ip

    async def _emit_document(self, in_ip: IPReader, root: Any, items: list[tuple[Any, Any]]) -> bool:
        if self.config.wrap_in_substream:
            open_ip = fbp_capnp.IP.new_message(type="openBracket")
            brackets.copy_attrs(in_ip, open_ip)
            if not await self.write_out("out", open_ip):
                return False

        inherited = self._parent_attrs(root)
        for index, (key, value) in enumerate(items):
            if not await self.write_out("out", self._item_ip(key, value, index, in_ip, inherited)):
                return False

        if not self.config.wrap_in_substream:
            return True

        close_attrs: dict[str, Any] = {self.config.count_attr: len(items)} if self.config.count_attr else {}
        return await self.write_out("out", brackets.make_bracket("closeBracket", close_attrs))

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        split = 0

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            try:
                document = json.loads(in_ip.content.as_text())
            except (capnp.KjException, json.JSONDecodeError, UnicodeDecodeError, ValueError):
                logger.warning("%s: content was not readable JSON text.", self.name)
                if self.config.on_error == "pass_through" and not await self.write_out("out", in_ip):
                    break
                continue

            # copy_parent_paths resolves against the document as received, not against whatever
            # traversal_path narrowed it to - "parent" would mean nothing otherwise.
            root = document
            if self.config.traversal_path:
                path = selectors.split_path(self.config.traversal_path, self.config.path_separator)
                resolved = selectors.apply_path(document, path)
                if resolved is values.MISSING:
                    logger.warning("%s: traversal_path %r did not resolve.", self.name, self.config.traversal_path)
                    if self.config.on_error == "pass_through" and not await self.write_out("out", in_ip):
                        break
                    continue
                document = resolved

            items = self._items_of(document)
            if items is None:
                # Not splittable in this mode - an atomic value, or an object in list mode.
                logger.debug("%s: content is not splittable in mode %r; forwarding it.", self.name, self.config.mode)
                if not await self.write_out("out", in_ip):
                    break
                continue

            split += 1
            if not await self._emit_document(in_ip, root, items):
                break

        logger.info("%s process finished, split %d document(s)", self.name, split)


def main():
    process.run_process_from_metadata_and_cmd_args(SplitJson(METADATA), METADATA)


if __name__ == "__main__":
    main()
