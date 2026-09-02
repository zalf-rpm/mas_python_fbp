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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, override

from mas.schema.common import common_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

_NUMERIC_VARIANTS = frozenset({"i8", "i16", "i32", "i64", "ui8", "ui16", "ui32", "ui64", "f32", "f64"})


class SortIPsConfig(process.ProcessConfig):
    sort_substreams: bool = Field(
        True,
        description="""If True, the 'in' stream is expected to be organized into open-/close-bracket delimited
        substreams. Each leaf-level substream (one without further nested substreams) is buffered and sorted by
        'sort_attr' before being forwarded; brackets of non-leaf substreams are just forwarded, only their nested
        leaf substreams get sorted. If False, the 'in' stream must not contain bracket IPs - if one is received
        anyway, it is dropped (and a warning logged); no sorting takes place and content IPs are forwarded as-is.""",
    )
    sort_attr: str = Field(
        "",
        description="""Name of the attribute (on each IP to sort) holding a common.capnp:Value to sort by. Its
        value has to be one of the number types (i8-i64, ui8-ui64, f32, f64), sorted numerically, or a string (t),
        sorted lexically. May optionally be prefixed with '@' (e.g. '@my_attr') for consistency with other
        components, though here it always refers to an attribute name either way. If empty, no sorting is
        performed and IPs are forwarded in the order received.""",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="0897bcb5-a611-4490-95d4-fbff21ee0ed8",
        name="Sort IPs",
        description="Sort IPs by an attribute holding a common.capnp:Value. Is substream sensitive.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="The IPs to sort. Substream sensitive, see 'sort_substreams'.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The (leaf-level) sorted IPs, see 'sort_substreams' and 'sort_attr'.",
        ),
    ],
    config=SortIPsConfig,
)


@dataclass
class _IPItem:
    ip: IPReader
    key: tuple[Literal[0, 1], object]


@dataclass
class _SubstreamItem:
    open_ip: IPReader
    items: list[_IPItem | _SubstreamItem]
    close_ip: IPReader | None


class SortIPs(process.Process[SortIPsConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        if self.config.sort_substreams:
            await self._run_substream_sensitive()
        else:
            await self._run_flat()

        logger.info("%s: process finished", self.name)

    async def _run_flat(self):
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if in_ip.type in ("openBracket", "closeBracket"):
                logger.warning(
                    "%s: dropping unexpected '%s' IP because 'sort_substreams' is False", self.name, in_ip.type
                )
                continue

            if not await self.write_out("out", in_ip):
                return

    async def _run_substream_sensitive(self):
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if in_ip.type == "closeBracket":
                logger.warning("%s: dropping unexpected stray 'closeBracket' IP", self.name)
                continue

            if in_ip.type != "openBracket":
                logger.warning(
                    "%s: dropping unexpected standalone IP received outside of a substream "
                    "('sort_substreams' is True, so open/close brackets are expected)",
                    self.name,
                )
                continue

            items, close_ip = await self._read_substream_items()
            if not await self.write_out("out", in_ip):
                return
            for out_ip in self._flatten_sorted(items):
                if not await self.write_out("out", out_ip):
                    return
            if close_ip is not None and not await self.write_out("out", close_ip):
                return

    async def _read_substream_items(self) -> tuple[list[_IPItem | _SubstreamItem], IPReader | None]:
        """Read (and recursively collect) IPs until (and including) the matching close bracket."""
        items: list[_IPItem | _SubstreamItem] = []
        while True:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                logger.warning("%s: 'in' port closed before a substream's matching close bracket arrived", self.name)
                return items, None

            if in_ip.type == "closeBracket":
                return items, in_ip

            if in_ip.type == "openBracket":
                nested_items, close_ip = await self._read_substream_items()
                items.append(_SubstreamItem(in_ip, nested_items, close_ip))
            else:
                items.append(_IPItem(in_ip, self._sort_key(in_ip)))

    def _flatten_sorted(self, items: list[_IPItem | _SubstreamItem]) -> list[IPReader]:
        # a level is only sorted if it's a leaf, i.e. it has no nested substreams of its own;
        # non-leaf levels are forwarded in the order received, recursing into their substreams
        if any(isinstance(item, _SubstreamItem) for item in items):
            out: list[IPReader] = []
            for item in items:
                if isinstance(item, _IPItem):
                    out.append(item.ip)
                else:
                    out.append(item.open_ip)
                    out.extend(self._flatten_sorted(item.items))
                    if item.close_ip is not None:
                        out.append(item.close_ip)
            return out

        ip_items = [item for item in items if isinstance(item, _IPItem)]
        try:
            ip_items = sorted(ip_items, key=lambda item: item.key)
        except TypeError:
            logger.exception(
                "%s: couldn't compare '%s' values of mixed type across this substream; forwarding unsorted",
                self.name,
                self.config.sort_attr,
            )
        return [item.ip for item in ip_items]

    def _sort_key(self, ip: IPReader) -> tuple[Literal[0, 1], object]:
        attr_name = self.config.sort_attr.removeprefix("@").strip()
        if not attr_name:
            # no sort_attr configured: same key for everyone, so sorted() is a stable no-op
            return (0, None)

        value = common.get_fbp_attr(ip, attr_name, common_capnp.Value.schema)
        if value is None:
            logger.warning("%s: IP is missing sort attribute '%s'; sorting it last", self.name, attr_name)
            return (1, None)

        which = value.which()
        if which in _NUMERIC_VARIANTS or which == "t":
            return (0, getattr(value, which))

        logger.warning(
            "%s: sort attribute '%s' has unsupported value type '%s'; sorting it last", self.name, attr_name, which
        )
        return (1, None)


def main():
    process.run_process_from_metadata_and_cmd_args(SortIPs(METADATA), METADATA)


if __name__ == "__main__":
    main()
