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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common import rect_ascii_grid_management as ragm

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.components.geo import _coord_types as coord_types
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    path_to_grid: str = Field(default="", description="Path to the lat/lon ASCII grid file.")
    type: Literal["int", "float"] = Field(default="int", description="Type to read grid values as.")
    to_attr: str | None = Field(
        default=None,
        description="Write the value to this attribute instead of replacing the content.",
    )
    return_no_data: bool = Field(
        default=False,
        description="Emit the grid's nodata value as well, instead of treating those cells as missing.",
    )
    on_missing: Literal["skip", "null", "fail"] = Field(
        default="skip",
        description=(
            "What to do when a coordinate falls outside the grid or on a nodata cell: drop the IP, "
            "emit it with no value, or stop the process."
        ),
    )
    debug_out: bool = Field(default=False, description="Log the grid path when it is loaded.")


METADATA = meta.Component(
    category=meta.Category(id="geo", name="Geo"),
    info=meta.Info(
        id="d8e349d6-e0e0-49cb-a24f-0b42358791a5",
        name="get lat/lon grid value",
        description=("Look a lat/lon coordinate up in an ASCII grid and emit the value there. Substream transparent."),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="@0xecf1fc3039cc8ffb = geo/geo.capnp:LatLonCoord",
            desc="Coordinates to look up.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="@0xe17592335373b246 = common/common.capnp:Value",
            desc="The grid value at each coordinate.",
            required=True,
        ),
    ],
    config=Config,
)


class GetLatLonGridValue(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def coord_of(self, in_ip: IPReader) -> Any | None:
        declared = values.resolve_schema(values.content_type_of(in_ip))
        schema = declared if declared is not None else coord_types.struct_type_for("latlon")
        if schema is None:
            return None
        try:
            return in_ip.content.as_struct(schema)
        except capnp.KjException:
            return None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if not self.config.path_to_grid:
            logger.error("%s: no 'path_to_grid' configured.", self.name)
            return
        if not Path(self.config.path_to_grid).is_file():
            logger.error("%s: no such grid file: %s", self.name, self.config.path_to_grid)
            return

        try:
            grid = ragm.load_grid_cached(
                self.config.path_to_grid,
                int if self.config.type == "int" else float,
                print_path=self.config.debug_out,
            )
        except (OSError, ValueError, KeyError):
            logger.exception("%s: could not load the grid %s", self.name, self.config.path_to_grid)
            return

        looked_up = 0
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
                logger.warning("%s: could not read a lat/lon coordinate from this IP.", self.name)
                continue

            # The grid's value() takes (lat, lon, return_no_data) - all three are required. Calling
            # it with two raised a TypeError for every IP, which a bare except then swallowed, so
            # this component silently emitted nothing at all.
            value = grid["value"](coord.lat, coord.lon, self.config.return_no_data)

            if value is None:
                message = f"{self.name}: no grid value at lat={coord.lat}, lon={coord.lon}"
                if self.config.on_missing == "fail":
                    raise ValueError(message)
                logger.info(message)
                if self.config.on_missing == "skip":
                    continue

            if value is None:
                cval = common_capnp.Value.new_message(t="")
            elif self.config.type == "int":
                cval = common_capnp.Value.new_message(i64=int(value))
            else:
                cval = common_capnp.Value.new_message(f64=float(value))

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if self.config.to_attr:
                out_ip.content = in_ip.content
                if (incoming := values.content_type_of(in_ip)) is not None:
                    out_ip.sysAttributes.contentType = incoming
                extra[self.config.to_attr] = brackets.Attr(cval, values.VALUE_TYPE)
            else:
                out_ip.content = cval
                out_ip.sysAttributes.contentType = values.VALUE_TYPE
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            looked_up += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, looked up %d coordinate(s)", self.name, looked_up)


def main():
    process.run_process_from_metadata_and_cmd_args(GetLatLonGridValue(METADATA), METADATA)


if __name__ == "__main__":
    main()
