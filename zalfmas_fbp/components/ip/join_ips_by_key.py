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
import time
from typing import TYPE_CHECKING, Any, Literal, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    selector: str = Field(
        default="@key",
        description=(
            "What identifies an IP's group: an attribute ('@id'), a content path ('./site/id'), or "
            "IP metadata. Same selector syntax as 'Filter IPs'."
        ),
    )
    primary: int = Field(
        default=0,
        description="Which input's content and attributes the joined IP is built from.",
    )
    combine: Literal["attributes", "json_object", "substream"] = Field(
        default="attributes",
        description=(
            "How to combine one IP per input: fold the others into attributes of the primary, "
            "build a JSON object keyed by input index, or emit them as a substream."
        ),
    )
    attr_prefix: str = Field(
        default="in",
        description="Attribute name prefix for the 'attributes' mode, giving in1, in2 and so on.",
    )
    key_attr: str | None = Field(
        default=None,
        description="If set, record the join key on the emitted IP as this attribute.",
    )
    timeout_seconds: float = Field(
        default=0.0,
        description=(
            "Give up on a group whose partners have not arrived within this long. 0 waits "
            "indefinitely. Checked when an IP arrives and when the inputs close, not on a timer, "
            "so an idle join is not interrupted."
        ),
    )
    max_pending: int = Field(
        default=10000,
        description=(
            "How many incomplete groups to hold before dropping the oldest. Guards against a key "
            "that never completes filling memory. 0 means no limit."
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
        id="d027a5fa-d61c-483d-874b-2f3a399410c4",
        name="Join IPs by key",
        description=(
            "Correlate IPs arriving on several inputs by a shared key and emit one joined IP per "
            "group. The synchronisation primitive for flows whose branches return out of order - "
            "positional zipping silently mispairs those."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            type="array",
            contentType="AnyPointer",
            desc="The streams to join. Slot order decides the attribute and JSON key numbering.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="One IP per complete group.", required=True),
        meta.Port(
            name="unmatched",
            contentType="AnyPointer",
            desc=(
                "IPs of groups that never completed - timed out, evicted, or still waiting when "
                "the inputs closed. Optional; they are dropped if it is unconnected."
            ),
            role="reject",
        ),
    ],
    config=Config,
)


class _Group:
    """The IPs seen so far for one key, by input slot."""

    __slots__ = ("first_seen", "ips")

    def __init__(self) -> None:
        self.ips: dict[int, IPReader] = {}
        self.first_seen: float = time.monotonic()


class JoinIPsByKey(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._pending: dict[Any, _Group] = {}

    def _key_of(self, in_ip: IPReader) -> Any:
        resolved = selectors.resolve(
            in_ip,
            self.config.selector,
            separator=self.config.path_separator,
            content_type=self.config.content_type,
            attr_types=self.config.attr_types,
        )
        return None if resolved is values.MISSING else resolved

    def _joined_ip(self, key: Any, group: _Group, slots: int) -> IPBuilder:
        ordered = [group.ips[index] for index in sorted(group.ips)]
        primary = group.ips.get(self.config.primary, ordered[0])

        out_ip = fbp_capnp.IP.new_message()
        extra: dict[str, Any] = {}
        if self.config.key_attr and key is not None:
            extra[self.config.key_attr] = key

        if self.config.combine == "json_object":
            combined = {
                str(index): values.python_from_content(in_ip, self.config.content_type)
                for index, in_ip in sorted(group.ips.items())
            }
            out_ip.content = json.dumps(
                {k: (None if v is values.MISSING else v) for k, v in combined.items()},
                default=str,
            )
            out_ip.sysAttributes.contentType = "Text (JSON)"
        else:
            out_ip.content = primary.content
            if primary.sysAttributes.contentType:
                out_ip.sysAttributes.contentType = primary.sysAttributes.contentType
            for index, in_ip in sorted(group.ips.items()):
                if index == self.config.primary:
                    continue
                extra[f"{self.config.attr_prefix}{index}"] = brackets.Attr(
                    value=in_ip.content,
                    value_type=in_ip.sysAttributes.contentType or None,
                )

        brackets.copy_attrs(primary, out_ip, extra=extra)
        return out_ip

    async def _emit(self, key: Any, group: _Group, slots: int) -> bool:
        if self.config.combine != "substream":
            return await self.write_out("out", self._joined_ip(key, group, slots))

        attrs = {self.config.key_attr: key} if self.config.key_attr and key is not None else {}
        if not await self.write_out("out", brackets.make_bracket("openBracket", attrs)):
            return False
        for _index, in_ip in sorted(group.ips.items()):
            if not await self.write_out("out", in_ip):
                return False
        return await self.write_out("out", brackets.make_bracket("closeBracket"))

    async def _release(self, key: Any, group: _Group, reason: str) -> bool:
        """Send an incomplete group's IPs to 'unmatched', or drop them."""
        logger.info("%s: releasing group %r (%s), %d IP(s)", self.name, key, reason, len(group.ips))
        if self.out_ports["unmatched"] is None:
            return True
        for _index, in_ip in sorted(group.ips.items()):
            if not await self.write_out("unmatched", in_ip):
                return False
        return True

    async def _evict_expired(self) -> bool:
        if self.config.timeout_seconds <= 0:
            return True
        cutoff = time.monotonic() - self.config.timeout_seconds
        for key in [k for k, group in self._pending.items() if group.first_seen < cutoff]:
            if not await self._release(key, self._pending.pop(key), "timed out"):
                return False
        return True

    async def _evict_oldest(self) -> bool:
        if self.config.max_pending <= 0 or len(self._pending) <= self.config.max_pending:
            return True
        key = next(iter(self._pending))
        return await self._release(key, self._pending.pop(key), "max_pending reached")

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        slots = len(self.array_in_ports.get("in", []))
        if slots < 2:
            logger.warning("%s has %d connected input(s); a join needs at least 2.", self.name, slots)

        joined = 0
        while self.out_ports["out"]:
            result = await self.read_array_in_with_index("in")
            if result is None:
                break
            index, in_ip = result

            if brackets.is_bracket(in_ip):
                logger.debug("%s: dropping a bracket IP; a join regroups by key.", self.name)
                continue

            key = self._key_of(in_ip)
            group = self._pending.setdefault(key, _Group())
            if index in group.ips:
                logger.warning(
                    "%s: input %d already had an IP for key %r; keeping the first.",
                    self.name,
                    index,
                    key,
                )
            else:
                group.ips[index] = in_ip

            if len(group.ips) >= slots:
                del self._pending[key]
                joined += 1
                if not await self._emit(key, group, slots):
                    break

            if not await self._evict_expired() or not await self._evict_oldest():
                break

        for key in list(self._pending):
            if not await self._release(key, self._pending.pop(key), "inputs closed"):
                break

        logger.info("%s process finished, joined %d group(s)", self.name, joined)


def main():
    process.run_process_from_metadata_and_cmd_args(JoinIPsByKey(METADATA), METADATA)


if __name__ == "__main__":
    main()
