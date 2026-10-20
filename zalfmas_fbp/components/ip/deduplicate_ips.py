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

import hashlib
import json
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Literal, override

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


class Config(process.ProcessConfig):
    selector: str | None = Field(
        default=None,
        description=(
            "What identifies an IP: an attribute ('@id'), a content path ('./site/id'), and so on. "
            "Null compares whole contents instead, by hash."
        ),
    )
    scope: Literal["stream", "substream"] = Field(
        default="stream",
        description="Remember what has been seen across the whole stream, or forget at each substream.",
    )
    window: int = Field(
        default=0,
        description=(
            "Remember only the last this many keys, so a long stream does not grow without bound. "
            "0 remembers everything."
        ),
    )
    path_separator: str = Field(default="/", description="Separator used inside the selector.")
    content_type: str | None = Field(
        default=None,
        description="Content type to assume for IPs that carry none, when the selector reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType; '*' covers all.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="34ee2532-85a7-41c3-8918-d293487592a5",
        name="Deduplicate IPs",
        description=(
            "Forward the first IP for each key and divert repeats. Substream transparent; the "
            "memory can be kept across the stream or reset per substream."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to deduplicate.", required=True)],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="The first IP seen for each key.", required=True),
        meta.Port(
            name="dup",
            contentType="AnyPointer",
            desc="Repeats. Optional; they are dropped if it is unconnected.",
            role="reject",
        ),
    ],
    config=Config,
)


class DeduplicateIPs(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._seen: OrderedDict[Any, None] = OrderedDict()

    def _key_of(self, in_ip: IPReader) -> Any:
        if self.config.selector:
            resolved = selectors.resolve(
                in_ip,
                self.config.selector,
                separator=self.config.path_separator,
                content_type=self.config.content_type,
                attr_types=self.config.attr_types,
            )
            if resolved is values.MISSING:
                return None
            return json.dumps(resolved, sort_keys=True, default=str)

        content = values.python_from_content(in_ip, self.config.content_type)
        if content is values.MISSING:
            # No readable content and no selector: nothing to compare, so let it through.
            return None
        return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()

    def _remember(self, key: Any) -> bool:
        """Record the key. Returns True if it had not been seen."""
        if key in self._seen:
            self._seen.move_to_end(key)
            return False
        self._seen[key] = None
        if self.config.window > 0:
            while len(self._seen) > self.config.window:
                _ = self._seen.popitem(last=False)
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        forwarded = 0
        duplicates = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if self.config.scope == "substream" and brackets.is_open_bracket(in_ip):
                    self._seen.clear()
                if not await self.write_out("out", in_ip):
                    break
                continue

            key = self._key_of(in_ip)
            if key is None or self._remember(key):
                forwarded += 1
                if not await self.write_out("out", in_ip):
                    break
            else:
                duplicates += 1
                if self.out_ports["dup"] is not None and not await self.write_out("dup", in_ip):
                    break

        logger.info(
            "%s process finished, forwarded %d and diverted %d duplicate(s)",
            self.name,
            forwarded,
            duplicates,
        )


def main():
    process.run_process_from_metadata_and_cmd_args(DeduplicateIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
