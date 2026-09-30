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
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.model.monica import monica_management_capnp as mgmt_capnp
from pydantic import Field
from pyproj import CRS
from zalfmas_common import common, geo
from zalfmas_services.management import ilr_sowing_harvest_dates as ilr

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

type TimeMode = Literal["fixed", "auto"]

ILR_DATES_TYPE = f"@0x{mgmt_capnp.ILRDates.schema.node.id:016x} = model/monica/monica_management.capnp:ILRDates"


class Config(process.ProcessConfig):
    path_to_ilr_csv: dict[str, str] = Field(
        default_factory=dict,
        description="Crop id -> path to that crop's ILR seed/harvest CSV, e.g. {'WW': '.../WW.csv'}.",
    )
    crop_ids: list[str] = Field(
        default_factory=lambda: ["WW", "SW", "WB"],
        description="Crop ids to load. Ids without a path in 'path_to_ilr_csv' are skipped.",
    )
    crop_id_attr: str = Field(default="cropId", description="Attribute holding the crop id.")
    latlon_attr: str = Field(default="latlon", description="Attribute holding the lat/lon coordinate.")
    sowing_time_attr: str = Field(
        default="sowingTime",
        description="Attribute holding 'fixed' or 'auto' for sowing.",
    )
    harvest_time_attr: str = Field(
        default="harvestTime",
        description="Attribute holding 'fixed' or 'auto' for harvest.",
    )
    to_attr: str | None = Field(
        default="ilr",
        description="Attribute to store the dates in. Null puts them in the content instead.",
    )
    forward_without_dates: bool = Field(
        default=True,
        description="Forward an IP for which no dates could be found, unchanged, instead of dropping it.",
    )


METADATA = meta.Component(
    category=meta.Category(id="management", name="Management"),
    info=meta.Info(
        id="bc9f8bfd-db77-49ed-a347-a26bb37084d1",
        name="ILR seed/harvest dates",
        description=(
            "Look up the ILR seed and harvest dates nearest a lat/lon location and attach them as "
            "management.capnp:ILRDates. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="IPs carrying a coordinate, crop id and the sowing/harvest modes as attributes.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="model/monica/monica_management.capnp:ILRDates",
            desc="The same IPs with the dates attached.",
            required=True,
        ),
    ],
    config=Config,
)


def _doy(entry: dict[str, int]) -> int:
    return date(2001, entry["month"], entry["day"]).timetuple().tm_yday


def ilr_date_fields(
    seed_harvest_data: dict[str, Any],
    is_winter_crop: bool,
    sowing_time: TimeMode,
    harvest_time: TimeMode,
) -> dict[str, Any]:
    """The ILRDates fields for one location, by sowing/harvest mode.

    Pure, so the date arithmetic - which is the substance of this component - can be checked
    against real ILR data without running a flow.
    """
    sowing_date = (
        seed_harvest_data["sowing-date"] if sowing_time == "fixed" else seed_harvest_data["latest-sowing-date"]
    )
    harvest_date = (
        seed_harvest_data["harvest-date"] if harvest_time == "fixed" else seed_harvest_data["latest-harvest-date"]
    )
    sdoy, hdoy = _doy(sowing_date), _doy(harvest_date)
    earliest_sowing = seed_harvest_data["earliest-sowing-date"]
    esd = date(2001, earliest_sowing["month"], earliest_sowing["day"])

    # A winter crop is harvested in the year after sowing, so its harvest may not run past the day
    # before sowing.
    harvest_doy = min(hdoy, sdoy - 1) if is_winter_crop else hdoy
    calc_harvest = date(2000, 12, 31) + timedelta(days=harvest_doy)
    harvest_entry = {"year": harvest_date["year"], "month": calc_harvest.month, "day": calc_harvest.day}

    if sowing_time == "fixed":
        return (
            {"sowing": seed_harvest_data["sowing-date"], "harvest": harvest_entry}
            if harvest_time == "fixed"
            else {"sowing": seed_harvest_data["sowing-date"], "latestHarvest": harvest_entry}
        )

    earliest_entry = (
        earliest_sowing if esd > date(esd.year, 6, 20) else {"year": sowing_date["year"], "month": 6, "day": 20}
    )
    if harvest_time == "fixed":
        calc_sowing = date(2000, 12, 31) + timedelta(days=max(hdoy + 1, sdoy))
        return {
            "earliestSowing": earliest_entry,
            "latestSowing": {"year": sowing_date["year"], "month": calc_sowing.month, "day": calc_sowing.day},
            "harvest": seed_harvest_data["harvest-date"],
        }
    return {
        "earliestSowing": earliest_entry,
        "latestSowing": seed_harvest_data["latest-sowing-date"],
        "latestHarvest": harvest_entry,
    }


class ILRSowingHarvestDates(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.by_crop: dict[str, Any] = {}

    def load_crops(self) -> None:
        wgs84, utm32n = CRS.from_epsg(4326), CRS.from_epsg(25832)
        for crop_id in self.config.crop_ids:
            path = self.config.path_to_ilr_csv.get(crop_id)
            if not path:
                logger.info("%s: no CSV configured for crop %r; skipping it.", self.name, crop_id)
                continue
            if not Path(path).is_file():
                logger.error("%s: no such ILR CSV for crop %r: %s", self.name, crop_id, path)
                continue
            try:
                self.by_crop[crop_id] = ilr.read_data_and_create_seed_harvest_geo_grid_interpolator(
                    crop_id,
                    path,
                    wgs84,
                    utm32n,
                )
                logger.info("%s: loaded ILR dates for crop %r from %s", self.name, crop_id, path)
            except (OSError, ValueError, KeyError):
                logger.exception("%s: could not read %s", self.name, path)

    def _text_attr(self, in_ip: IPReader, name: str) -> str | None:
        kv = values.attr_reader(in_ip, name)
        if kv is None:
            return None
        value = values.python_from_attr(kv)
        return None if value is values.MISSING or not isinstance(value, str) else value

    def _coord_of(self, in_ip: IPReader) -> Any | None:
        kv = values.attr_reader(in_ip, self.config.latlon_attr)
        if kv is None:
            return None
        try:
            return kv.value.as_struct(geo_capnp.LatLonCoord)
        except capnp.KjException:
            return None

    def dates_for(self, crop_id: str, coord: Any, sowing_time: str, harvest_time: str) -> dict[str, Any] | None:
        """The ILRDates fields for a coordinate, or None when there are none for it."""
        loaded = self.by_crop.get(crop_id)
        if loaded is None:
            logger.warning("%s: no ILR data loaded for crop %r.", self.name, crop_id)
            return None

        interpolate = loaded["interpolate"]
        if interpolate is None:
            return None

        # transform_from_to_geo_coord returns whichever coordinate struct the target names, so its
        # type is only known at runtime; utm32n gives a UTMCoord, which has r and h.
        utm: Any = geo.transform_from_to_geo_coord(coord, "utm32n")
        if utm is None:
            return None
        station = interpolate(utm.r, utm.h)
        if station is None:
            return None

        # The interpolator hands back a 0-d numpy array while the data is keyed by Python ints, so
        # indexing with it raised "unhashable type: numpy.ndarray" for every IP - which a bare
        # except then swallowed, leaving this component emitting nothing at all.
        entry = loaded["data"].get(int(station))
        if not entry:
            return None

        if sowing_time not in ("fixed", "auto") or harvest_time not in ("fixed", "auto"):
            logger.warning(
                "%s: sowing/harvest time must be 'fixed' or 'auto', got %r/%r.",
                self.name,
                sowing_time,
                harvest_time,
            )
            return None

        return ilr_date_fields(entry, loaded["is-winter-crop"], sowing_time, harvest_time)  # pyright: ignore[reportArgumentType]

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        self.load_crops()
        if not self.by_crop:
            logger.error("%s: no ILR data could be loaded; nothing to look up.", self.name)
            return

        found = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                # The previous version 'continue'd here without clearing the port, so it spun on an
                # exhausted input forever rather than finishing.
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            coord = self._coord_of(in_ip)
            crop_id = self._text_attr(in_ip, self.config.crop_id_attr)
            sowing_time = self._text_attr(in_ip, self.config.sowing_time_attr)
            harvest_time = self._text_attr(in_ip, self.config.harvest_time_attr)

            fields = None
            if coord is None or not crop_id or not sowing_time or not harvest_time:
                logger.warning("%s: IP is missing the coordinate, crop id or sowing/harvest mode.", self.name)
            else:
                fields = self.dates_for(crop_id, coord, sowing_time, harvest_time)

            if fields is None and not self.config.forward_without_dates:
                continue

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if fields is not None:
                dates = mgmt_capnp.ILRDates.new_message(**fields)
                found += 1
                if self.config.to_attr:
                    out_ip.content = in_ip.content
                    extra[self.config.to_attr] = brackets.Attr(dates, ILR_DATES_TYPE)
                else:
                    out_ip.content = dates
                    out_ip.sysAttributes.contentType = ILR_DATES_TYPE
            else:
                out_ip.content = in_ip.content
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, found dates for %d IP(s)", self.name, found)


def main():
    process.run_process_from_metadata_and_cmd_args(ILRSowingHarvestDates(METADATA), METADATA)


if __name__ == "__main__":
    main()
