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

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.grid import grid_capnp
from pydantic import Field, field_validator
from pymep.realParser import eval as mep_eval
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

GRID_VALUE_TYPE = "grid.capnp:Grid.Value"
COMMON_VALUE_TYPE = values.VALUE_TYPE


class Config(process.ProcessConfig):
    from_attr: str | None = Field(
        default=None,
        description="Read the coordinate from this attribute instead of the content.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Write the value to this attribute instead of the IP's content.",
    )
    as_common_value: bool = Field(
        default=False,
        description="Send a common.capnp:Value rather than a grid.capnp:Grid.Value.",
    )
    calc: str = Field(
        default="",
        description=(
            "Arithmetic expression applied to each value, e.g. 'v*0.1'. Empty passes the value "
            "through. Variable names must be a single letter - the expression parser silently "
            "evaluates longer names to 0."
        ),
    )
    calc_variable: str = Field(
        default="v",
        description="Single letter the grid value is bound to in 'calc'.",
    )
    calc_constants: dict[str, float] = Field(
        default_factory=dict,
        description="Further single-letter variables usable in 'calc'.",
    )
    ignore_no_data: bool = Field(
        default=True,
        description="Ask the grid to skip no-data cells and return the closest cell that has data.",
    )
    on_no_data: Literal["emit", "skip", "fail"] = Field(
        default="emit",
        description="What to do when the grid answers with a no-data value.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP holding no readable coordinate is skipped or stops the process.",
    )

    @field_validator("calc_variable")
    @classmethod
    def _single_letter(cls, value: str) -> str:
        if len(value) != 1 or not value.isalpha():
            msg = f"'calc_variable' must be a single letter, not {value!r}; the parser cannot resolve longer names"
            raise ValueError(msg)
        return value

    @field_validator("calc_constants")
    @classmethod
    def _single_letter_keys(cls, value: dict[str, float]) -> dict[str, float]:
        bad = sorted(name for name in value if len(name) != 1 or not name.isalpha())
        if bad:
            msg = f"'calc_constants' names must be single letters; these are not: {bad}"
            raise ValueError(msg)
        return value


METADATA = meta.Component(
    category=meta.Category(id="grid", name="Grid"),
    info=meta.Info(
        id="cb6720d6-bc33-445d-b2c1-aa3842219c81",
        name="Use grid service",
        description=(
            "Ask a grid service for the value closest to each incoming coordinate. The value can "
            "be passed through an arithmetic expression and sent either as a grid value or as a "
            "common value. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="geo.capnp:LatLonCoord",
            desc="The coordinate to get the value at.",
            required=True,
        ),
        meta.Port(
            name="service",
            contentType="grid.capnp:Grid | SturdyRef",
            desc="Capability or sturdy ref to the grid. Read once, before the first coordinate.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=f"{GRID_VALUE_TYPE} | common.capnp:Value",
            desc="The grid value at the given coordinate.",
            required=True,
        ),
    ],
    config=Config,
)


def number_of(grid_value: Any) -> float | int | None:
    """The value as a plain number, or None for a no-data cell."""

    match grid_value.which():
        case "f":
            return grid_value.f
        case "i":
            return grid_value.i
        case "ui":
            return grid_value.ui
        case _:
            return None


class UseGridService(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    def coord_of(self, in_ip: IPReader) -> Any | None:
        """The incoming coordinate, from an attribute or the content."""

        if self.config.from_attr:
            kv = values.attr_reader(in_ip, self.config.from_attr)
            if kv is None:
                return None
            pointer = kv.value
        else:
            pointer = in_ip.content
        try:
            return pointer.as_struct(geo_capnp.LatLonCoord)
        except capnp.KjException:
            return None

    def calculated(self, grid_value: Any) -> Any:
        """A new Grid.Value with the expression applied, keeping the original's union field.

        The old code assigned to the *reader* returned by the service, which cannot be written to,
        and passed `{name, value}` - a set literal, not a dict - as the variable table.
        """

        number = number_of(grid_value)
        if not self.config.calc or number is None:
            return grid_value

        variables = dict(self.config.calc_constants)
        variables[self.config.calc_variable] = float(number)
        result = mep_eval(self.config.calc, variables)

        out = grid_capnp.Grid.Value.new_message()
        match grid_value.which():
            case "f":
                out.f = float(result)
            case "i":
                out.i = int(result)
            case "ui":
                out.ui = max(0, int(result))
        return out

    def payload(self, grid_value: Any) -> tuple[Any, str]:
        """The outgoing value and the content type naming it."""

        value = self.calculated(grid_value)
        if not self.config.as_common_value:
            return value, GRID_VALUE_TYPE

        number = number_of(value)
        if number is None:
            return common_capnp.Value.new_message(), COMMON_VALUE_TYPE
        match value.which():
            case "f":
                return common_capnp.Value.new_message(f64=float(number)), COMMON_VALUE_TYPE
            case "i":
                return common_capnp.Value.new_message(i64=int(number)), COMMON_VALUE_TYPE
            case _:
                return common_capnp.Value.new_message(ui64=int(number)), COMMON_VALUE_TYPE

    async def grid_from_port(self) -> Any | None:
        """The one grid capability this component works against, read before the first coordinate."""

        service_ip = await self.read_in("service")
        if service_ip is None:
            self.in_ports["service"] = None
            logger.error("%s: the 'service' port closed before sending a grid.", self.name)
            return None
        grid, _ = await self.cast_cap_or_connect(service_ip.content, grid_capnp.Grid)
        if grid is None:
            logger.error("%s: no grid service could be received or connected to.", self.name)
        return grid

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        grid = await self.grid_from_port()
        if grid is None:
            return

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            coord = self.coord_of(in_ip)
            if coord is None:
                message = f"{self.name}: no coordinate could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            grid_value = (await grid.closestValueAt(coord, ignoreNoData=self.config.ignore_no_data)).val

            if number_of(grid_value) is None and self.config.on_no_data != "emit":
                message = f"{self.name}: the grid has no data at ({coord.lat}, {coord.lon})"
                if self.config.on_no_data == "fail":
                    raise ValueError(message)
                logger.info(message)
                continue

            payload, content_type = self.payload(grid_value)

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if self.config.to_attr:
                extra[self.config.to_attr] = brackets.Attr(payload, content_type)
            else:
                out_ip.content = payload
                out_ip.sysAttributes.contentType = content_type
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            self.sent += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, sent %d value(s)", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(UseGridService(METADATA), METADATA)


if __name__ == "__main__":
    main()
