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
from pathlib import Path
from typing import Any, Literal, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common import rect_ascii_grid_management as grid

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

JSON_CONTENT_TYPE = "Text (JSON)"

#: Bounds per region. 'earth' differs by resolution because the grids do.
REGION_BOUNDS: dict[str, dict[str, Any]] = {
    "nigeria": {"tl": {"lat": 14.0, "lon": 2.7}, "br": {"lat": 4.25, "lon": 14.7}},
    "africa": {"tl": {"lat": 37.4, "lon": -17.55}, "br": {"lat": -34.9, "lon": 51.5}},
    "earth": {
        "5min": {
            "tl": {"lat": 83.95833588, "lon": -179.95832825},
            "br": {"lat": -55.95833206, "lon": 179.50000000},
        },
        "30sec": {
            "tl": {"lat": 83.99578094, "lon": -179.99583435},
            "br": {"lat": -55.99583435, "lon": 179.99568176},
        },
    },
}

RESOLUTIONS: dict[str, float] = {"5min": 5 / 60.0, "30sec": 30 / 3600.0}
SCALE_FACTORS: dict[str, float] = {"5min": 60.0, "30sec": 3600.0}


class Config(process.ProcessConfig):
    region: str = Field(
        default="africa",
        description="Region to cover: nigeria, africa, earth, or any key of 'custom_bounds'.",
    )
    resolution: Literal["5min", "30sec"] = Field(default="5min", description="Grid resolution to step by.")
    custom_bounds: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Extra regions, as {'name': {'tl': {'lat':, 'lon':}, 'br': {'lat':, 'lon':}}}. Takes "
            "precedence over the built-in ones."
        ),
    )
    ids: list[int] = Field(
        default_factory=list,
        description="Keep only cells whose id-grid value is in this list. Empty keeps all of them.",
    )
    path_to_ids_grid: str = Field(
        default="",
        description=(
            "ASCII grid of ids, used to filter cells and to label each coordinate. Empty emits "
            "every cell in the bounds with no id."
        ),
    )
    stream: bool = Field(
        default=False,
        description="Emit one IP per coordinate. Off emits one JSON array of [lat, lon, id] per region.",
    )
    create_substream: bool = Field(
        default=False,
        description="Wrap each streamed region in an open-/close-bracket pair. Only used with 'stream'.",
    )


METADATA = meta.Component(
    category=meta.Category(id="geo", name="Geo"),
    info=meta.Info(
        id="1229ed4f-9fef-4b76-9061-a117d52e9bc2",
        name="create lat lon coords",
        description=(
            "Generate the lat/lon coordinates covering a region at a given resolution, optionally "
            "filtered by an id grid. Emits one IP per coordinate, or one JSON array per region."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="region",
            contentType="Text",
            desc="Optional. A region name per IP, overriding the configured one.",
            role="control",
        ),
        meta.Port(
            name="ids",
            contentType="Text (JSON) | common.capnp:Value",
            desc="Optional. A list of ids per IP, overriding the configured one.",
            role="control",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="common.capnp:Pair(Value, geo.capnp:LatLonCoord) | Text (JSON)",
            desc="Coordinates, streamed or as one JSON array per region.",
            required=True,
        ),
    ],
    config=Config,
)


def bounds_for(region: str, resolution: str, custom: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """The top-left/bottom-right bounds of a region, or None if it is not known.

    'earth' is keyed by resolution rather than holding tl/br directly, which the previous version
    did not account for.
    """
    entry = (custom or {}).get(region, REGION_BOUNDS.get(region))
    if entry is None:
        return None
    if "tl" in entry and "br" in entry:
        return entry
    return entry.get(resolution)


def coordinates_in(bounds: dict[str, Any], resolution: str) -> list[tuple[float, float]]:
    """Every (lat, lon) in the bounds, stepping by the resolution."""
    step, scale = RESOLUTIONS[resolution], SCALE_FACTORS[resolution]
    lats = range(
        int(bounds["tl"]["lat"] * scale),
        int(bounds["br"]["lat"] * scale) - 1,
        -int(step * scale),
    )
    lons = range(
        int(bounds["tl"]["lon"] * scale),
        int(bounds["br"]["lon"] * scale) + 1,
        int(step * scale),
    )
    return [(lat / scale, lon / scale) for lat in lats for lon in lons]


class CreateLatLonCoords(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self._ids_grid: Any = None

    def _load_ids_grid(self) -> bool:
        if not self.config.path_to_ids_grid:
            return True
        if not Path(self.config.path_to_ids_grid).is_file():
            logger.error("%s: no such id grid: %s", self.name, self.config.path_to_ids_grid)
            return False
        try:
            self._ids_grid = grid.load_grid_cached(self.config.path_to_ids_grid, int)
        except (OSError, ValueError, KeyError):
            logger.exception("%s: could not load the id grid", self.name)
            return False
        return True

    def _id_at(self, lat: float, lon: float) -> int | None:
        if self._ids_grid is None:
            return None
        return self._ids_grid["value"](lat, lon, False)

    def _keeps(self, cell_id: int | None, wanted: list[int]) -> bool:
        if self._ids_grid is None:
            return True
        if not cell_id:
            return False
        return not wanted or cell_id in wanted

    async def _emit_region(self, region: str, wanted: list[int]) -> bool:
        bounds = bounds_for(region, self.config.resolution, self.config.custom_bounds)
        if bounds is None:
            logger.error("%s: unknown region %r.", self.name, region)
            return True

        if self.config.stream and self.config.create_substream:
            if not await self.write_out("out", brackets.make_bracket("openBracket", {"region": region})):
                return False

        collected: list[list[Any]] = []
        emitted = 0
        for lat, lon in coordinates_in(bounds, self.config.resolution):
            cell_id = self._id_at(lat, lon)
            if not self._keeps(cell_id, wanted):
                continue

            if self.config.stream:
                pair = common_capnp.Pair.new_message(
                    fst=common_capnp.Value.new_message(i64=cell_id or 0),
                    snd=geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon),
                )
                out_ip = fbp_capnp.IP.new_message(content=pair)
                brackets.set_attrs(out_ip, {"region": region})
                if not await self.write_out("out", out_ip):
                    return False
            else:
                # The previous version appended the *builtin* `id` here rather than the cell's id,
                # so json.dumps raised and this whole path emitted nothing.
                collected.append([lat, lon, cell_id])
            emitted += 1

        logger.info("%s: %d coordinate(s) for region %r", self.name, emitted, region)

        if not self.config.stream:
            out_ip = fbp_capnp.IP.new_message(content=json.dumps(collected))
            out_ip.sysAttributes.contentType = JSON_CONTENT_TYPE
            brackets.set_attrs(out_ip, {"region": region, "count": emitted})
            return await self.write_out("out", out_ip)

        if self.config.create_substream:
            return await self.write_out("out", brackets.make_bracket("closeBracket", {"substream_length": emitted}))
        return True

    def _ids_from(self, in_ip: Any) -> list[int]:
        resolved = values.python_from_content(in_ip)
        if isinstance(resolved, str):
            try:
                resolved = json.loads(resolved)
            except (json.JSONDecodeError, ValueError):
                return []
        if isinstance(resolved, list):
            return [int(v) for v in resolved if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return []

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if not self._load_ids_grid():
            return

        region, wanted = self.config.region, list(self.config.ids)
        driven = self.in_ports["region"] is not None or self.in_ports["ids"] is not None

        if not driven:
            # No driving ports: emit once for the configured region, like any other source.
            _ = await self._emit_region(region, wanted)
            logger.info("%s process finished", self.name)
            return

        while self.out_ports["out"] and (self.in_ports["region"] or self.in_ports["ids"]):
            if self.in_ports["region"]:
                region_ip = await self.read_in("region")
                if region_ip is None:
                    self.in_ports["region"] = None
                    continue
                region = region_ip.content.as_text()

            if self.in_ports["ids"]:
                ids_ip = await self.read_in("ids")
                if ids_ip is None:
                    self.in_ports["ids"] = None
                    continue
                wanted = self._ids_from(ids_ip)

            if not await self._emit_region(region, wanted):
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(CreateLatLonCoords(METADATA), METADATA)


if __name__ == "__main__":
    main()
