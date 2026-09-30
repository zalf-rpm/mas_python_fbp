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
from zalfmas_common import common, geo

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    to_name: str = Field(
        default="LatLon",
        description=("One of 2D, XY, LatLon, WGS84, GKx (x=2-5), UTMab (a=[1-60], b=[C-X]). Case does not matter."),
    )
    list_type: Literal["float", "int"] = Field(
        default="float",
        description="Element type to read a raw Cap'n Proto list content as.",
    )
    x_index: int = Field(default=0, description="Position of the x / lon / easting value.")
    y_index: int = Field(default=1, description="Position of the y / lat / northing value.")
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP that cannot be read as a pair of numbers is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="geo", name="Geo"),
    info=meta.Info(
        id="66ea3fce-80f7-4ab6-b77a-0966cb7c2793",
        name="to geo coord",
        description=(
            "Create a geo.capnp coordinate (LatLonCoord, UTMCoord or GKCoord) from a pair of "
            "numbers. Accepts a raw Cap'n Proto list, a common.capnp:Value list, or JSON text. "
            "Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="vals",
            contentType="List[float | int] | common.capnp:Value | Text (JSON)",
            desc="The values to convert into a coordinate.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="coord",
            contentType="geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
            desc="The coordinate.",
            required=True,
        ),
    ],
    config=Config,
)


class ToGeoCoord(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _list_schema(self) -> Any:
        element = capnp.types.Float64 if self.config.list_type == "float" else capnp.types.Int64
        return capnp._ListSchema(element)  # noqa: SLF001 - the only way to build a list schema

    def values_of(self, in_ip: IPReader) -> list[float] | None:
        """The numbers carried by an IP, whichever of the accepted shapes it uses."""
        resolved = values.python_from_content(in_ip)
        if isinstance(resolved, str):
            try:
                resolved = json.loads(resolved)
            except (json.JSONDecodeError, ValueError):
                resolved = values.MISSING
        if isinstance(resolved, list):
            return [v for v in resolved if isinstance(v, (int, float)) and not isinstance(v, bool)]

        # A raw Cap'n Proto list carries no content type, so it is read with the configured one.
        try:
            return list(in_ip.content.as_list(self._list_schema()))
        except (capnp.KjException, TypeError):
            return None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        # zalfmas_common.geo.name_to_struct_type lowercases the name for '2d'/'xy'/'latlon' but
        # compares the raw string for 'wgs84', 'gk*' and 'utm*', so the capitalised forms this
        # component has always documented did not work. Normalising here makes all of them work
        # without needing a change upstream.
        template = geo.name_to_struct_instance(self.config.to_name.lower())
        if template is None:
            logger.error(
                "%s: %r is not a known coordinate name; use 2D, XY, LatLon, WGS84, GKx or UTMab.",
                self.name,
                self.config.to_name,
            )
            return

        converted = 0
        while self.in_ports["vals"] and self.out_ports["coord"]:
            in_ip = await self.read_in("vals")
            if in_ip is None:
                self.in_ports["vals"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("coord", in_ip):
                    break
                continue

            numbers = self.values_of(in_ip)
            needed = max(self.config.x_index, self.config.y_index) + 1
            if numbers is None or len(numbers) < needed:
                message = f"{self.name}: need {needed} numbers for a coordinate, got {numbers}"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            coord = template.copy()
            geo.set_xy(coord, numbers[self.config.x_index], numbers[self.config.y_index])
            out_ip = fbp_capnp.IP.new_message(content=coord)
            brackets.copy_attrs(in_ip, out_ip)

            converted += 1
            if not await self.write_out("coord", out_ip):
                break

        logger.info("%s process finished, converted %d coordinate(s)", self.name, converted)


def main():
    process.run_process_from_metadata_and_cmd_args(ToGeoCoord(METADATA), METADATA)


if __name__ == "__main__":
    main()
