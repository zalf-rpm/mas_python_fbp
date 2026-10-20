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

from mas.schema.fbp import fbp_capnp
from pydantic import BaseModel, ConfigDict, Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

type AggregationOp = Literal["count", "sum", "mean", "min", "max", "first", "last", "list", "set", "concat_text"]


class Aggregation(BaseModel):
    """One value to compute over a substream."""

    model_config = ConfigDict(extra="forbid")

    selector: str = Field(
        default=".",
        description="What to read from each IP: '@attr', './path', '#type', or '.' for the content.",
    )
    op: AggregationOp = Field(default="count", description="How to combine the values read.")
    to_attr: str | None = Field(
        default=None,
        description="Attribute on the emitted IP to hold the result. Also names it in JSON output.",
    )
    to_content: bool = Field(default=False, description="Put the result in the emitted IP's content.")
    separator: str = Field(default=", ", description="Joiner for the 'concat_text' op.")


class Config(process.ProcessConfig):
    aggregations: list[Aggregation] = Field(
        default_factory=lambda: [Aggregation(op="count", to_attr="count", to_content=True)],
        description="The values to compute over each substream.",
    )
    nesting_level: int = Field(
        0,
        description="Which bracket depth to reduce at. 0 reduces the outermost substreams.",
    )
    keep_open_bracket_attrs: bool = Field(
        True,
        description="Carry the substream's open-bracket attributes onto the emitted IP.",
    )
    pass_through_ips: bool = Field(
        False,
        description="Also forward the original IPs, emitting the reduction in place of the close-bracket.",
    )
    path_separator: str = Field("/", description="Separator used inside selectors.")
    content_type: str | None = Field(
        None,
        description="Content type to assume for IPs that carry none, when a selector reads content.",
    )
    attr_types: dict[str, str] = Field(
        default_factory=dict,
        description="Cap'n Proto types for attributes written without a valueType, by attribute name.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="e1f2e15f-dd1f-4d3f-8d06-dd675e6a1abe",
        name="Reduce substream",
        description=(
            "Collapse each substream into a single IP carrying aggregates over it. The general "
            "form of 'Concat JSON substream', working on attributes and content alike."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(name="in", contentType="AnyPointer", desc="Bracketed substreams to reduce.", required=True),
    ],
    outPorts=[
        meta.Port(name="out", contentType="AnyPointer", desc="One IP per reduced substream.", required=True),
    ],
    config=Config,
)


def _numbers(collected: list[Any]) -> list[float]:
    numeric: list[float] = []
    for value in collected:
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            numeric.append(float(value))
        elif isinstance(value, str):
            try:
                numeric.append(float(value.strip()))
            except ValueError:
                continue
    return numeric


def apply_op(op: AggregationOp, collected: list[Any], separator: str = ", ") -> Any:
    """Combine the values read from a substream's IPs. Returns None when it cannot be computed."""
    if op == "count":
        return len(collected)
    if not collected:
        return None
    if op == "first":
        return collected[0]
    if op == "last":
        return collected[-1]
    if op == "list":
        return collected
    if op == "set":
        seen: list[Any] = []
        for value in collected:
            if value not in seen:
                seen.append(value)
        return seen
    if op == "concat_text":
        return separator.join(str(value) for value in collected)

    numeric = _numbers(collected)
    if not numeric:
        return None
    if op == "sum":
        return sum(numeric)
    if op == "mean":
        return sum(numeric) / len(numeric)
    if op == "min":
        return min(numeric)
    return max(numeric)


class ReduceSubstream(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _read(self, in_ip: IPReader, selector: str) -> Any:
        resolved = selectors.resolve(
            in_ip,
            selector,
            separator=self.config.path_separator,
            content_type=self.config.content_type,
            attr_types=self.config.attr_types,
        )
        return None if resolved is values.MISSING else resolved

    def _results_for(self, ips: list[IPReader]) -> dict[str, Any]:
        """Aggregation name -> value, named by to_attr or by position when it has none."""
        results: dict[str, Any] = {}
        for index, aggregation in enumerate(self.config.aggregations):
            collected = [self._read(in_ip, aggregation.selector) for in_ip in ips]
            collected = [value for value in collected if value is not None] if aggregation.op != "count" else collected
            name = aggregation.to_attr or f"{aggregation.op}{index}"
            results[name] = apply_op(aggregation.op, collected, aggregation.separator)
        return results

    def _reduced_ip(self, substream: brackets.Substream) -> Any:
        ips = substream.ips
        results = self._results_for(ips)

        to_content = [
            (aggregation.to_attr or f"{aggregation.op}{index}")
            for index, aggregation in enumerate(self.config.aggregations)
            if aggregation.to_content
        ]

        out_ip = fbp_capnp.IP.new_message()
        if len(to_content) == 1:
            content = results[to_content[0]]
            out_ip.content = values.value_from_python(content) if content is not None else values.value_from_python("")
            out_ip.sysAttributes.contentType = values.VALUE_TYPE
        elif to_content:
            out_ip.content = json.dumps({name: results[name] for name in to_content}, default=str)
            out_ip.sysAttributes.contentType = "Text (JSON)"

        attrs: dict[str, Any] = {}
        if self.config.keep_open_bracket_attrs and substream.open_ip is not None:
            attrs.update(brackets.attr_readers(substream.open_ip))
        for index, aggregation in enumerate(self.config.aggregations):
            if aggregation.to_attr:
                value = results[aggregation.to_attr]
                attrs[aggregation.to_attr] = value if value is not None else ""
        brackets.set_attrs(out_ip, attrs)
        return out_ip

    async def _reduce_at_level(self, substream: brackets.Substream, depth: int) -> bool:
        """Emit the reduction of every substream sitting at the configured nesting level."""
        if depth == self.config.nesting_level:
            return await self.write_out("out", self._reduced_ip(substream))

        # Above the level of interest: keep the grouping and recurse into it.
        if substream.open_ip is not None and not await self.write_out("out", substream.open_ip):
            return False
        for item in substream.items:
            if isinstance(item, brackets.Substream):
                if not await self._reduce_at_level(item, depth + 1):
                    return False
            elif not await self.write_out("out", item):
                return False
        if substream.close_ip is not None and not await self.write_out("out", substream.close_ip):
            return False
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        reduced = 0

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if not brackets.is_open_bracket(in_ip):
                logger.warning(
                    "%s: IP of type %r arrived outside a substream; forwarding it unchanged.",
                    self.name,
                    str(in_ip.type),
                )
                if not await self.write_out("out", in_ip):
                    break
                continue

            substream = await brackets.collect_substream(lambda: self.read_in("in"), in_ip)
            if substream.truncated:
                logger.warning("%s: input closed mid-substream; reducing what arrived.", self.name)
                self.in_ports["in"] = None

            if self.config.pass_through_ips:
                for item in [substream.open_ip, *substream.all_ips()]:
                    if item is not None and not await self.write_out("out", item):
                        return

            reduced += 1
            if not await self._reduce_at_level(substream, 0):
                break

        logger.info("%s process finished, reduced %d substream(s)", self.name, reduced)


def main():
    process.run_process_from_metadata_and_cmd_args(ReduceSubstream(METADATA), METADATA)


if __name__ == "__main__":
    main()
