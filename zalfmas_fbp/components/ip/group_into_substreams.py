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

_ABSENT = object()


class Config(process.ProcessConfig):
    selector: str = Field(
        "@group_key",
        description=(
            "What to group by: an attribute ('@region'), a content path ('./site/id'), or IP "
            "metadata ('#contentType'). Same selector syntax as 'Filter IPs'."
        ),
    )
    mode: Literal["on_key_change", "buffer_all"] = Field(
        "on_key_change",
        description=(
            "'on_key_change' closes a group as soon as the key changes, which needs the stream "
            "sorted by key but holds only one group in memory. 'buffer_all' buffers everything and "
            "emits one substream per distinct key, in first-seen order."
        ),
    )
    key_attr: str | None = Field(
        default="group_key",
        description="Attach the group's key to its open-bracket as this attribute. Null or empty disables it.",
    )
    max_group_size: int = Field(
        0,
        description="Split groups larger than this into several substreams. 0 means no limit.",
    )
    path_separator: str = Field("/", description="Separator used inside the selector.")
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none, when the selector reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType, by attribute name.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="7553a3cb-8804-49d6-9dd6-f13759055534",
        name="Group IPs into substreams",
        description=(
            "Wrap runs of IPs sharing a key into substreams. Where 'Wrap IPs into substream' groups "
            "by count or an external bracket channel, this groups by the data itself."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to group.", required=True)],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="The same IPs, in substreams.", required=True),
    ],
    config=Config,
)


class GroupIntoSubstreams(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._open_key: Any = _ABSENT
        self._open_size: int = 0

    def _key_of(self, in_ip: IPReader) -> Any:
        resolved = selectors.resolve(
            in_ip,
            self.config.selector,
            separator=self.config.path_separator,
            content_type=self.config.content_type,
            attr_types=self.config.attr_types,
        )
        return None if resolved is values.MISSING else resolved

    async def _open_group(self, key: Any) -> bool:
        attrs = {self.config.key_attr: key} if self.config.key_attr and key is not None else {}
        self._open_key = key
        self._open_size = 0
        return await self.write_out("out", brackets.make_bracket("openBracket", attrs))

    async def _close_group(self) -> bool:
        if self._open_key is _ABSENT:
            return True
        self._open_key = _ABSENT
        return await self.write_out("out", brackets.make_bracket("closeBracket"))

    async def _run_on_key_change(self) -> int:
        """Close a group as soon as the key changes. One group in memory, never more."""
        grouped = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                # Incoming grouping is replaced by ours; keeping both would nest unpredictably.
                logger.debug("%s: dropping an incoming bracket IP, this component regroups.", self.name)
                continue

            key = self._key_of(in_ip)
            oversized = 0 < self.config.max_group_size <= self._open_size
            if self._open_key is _ABSENT or key != self._open_key or oversized:
                if not await self._close_group():
                    break
                if not await self._open_group(key):
                    break
                grouped += 1

            self._open_size += 1
            if not await self.write_out("out", in_ip):
                break

        await self._close_group()
        return grouped

    async def _run_buffer_all(self) -> int:
        """Buffer the whole stream and emit one substream per distinct key, first-seen order."""
        groups: dict[Any, list[IPReader]] = {}
        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break
            if brackets.is_bracket(in_ip):
                continue
            groups.setdefault(self._key_of(in_ip), []).append(in_ip)

        emitted = 0
        limit = self.config.max_group_size
        for key, ips in groups.items():
            chunks = [ips[i : i + limit] for i in range(0, len(ips), limit)] if limit > 0 else [ips]
            for chunk in chunks:
                if not await self._open_group(key):
                    return emitted
                for in_ip in chunk:
                    if not await self.write_out("out", in_ip):
                        return emitted
                if not await self._close_group():
                    return emitted
                emitted += 1
        return emitted

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.config.mode == "buffer_all":
            grouped = await self._run_buffer_all()
        else:
            grouped = await self._run_on_key_change()

        logger.info("%s process finished, emitted %d substream(s)", self.name, grouped)


def main():
    process.run_process_from_metadata_and_cmd_args(GroupIntoSubstreams(METADATA), METADATA)


if __name__ == "__main__":
    main()
