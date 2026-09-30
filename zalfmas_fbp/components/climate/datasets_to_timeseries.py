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
from typing import Any, Literal, override

import capnp
from mas.schema.climate import climate_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

TIMESERIES_TYPE = "climate.capnp:TimeSeries"


class Config(process.ProcessConfig):
    no_of_locations_at_once: int = Field(
        default=10,
        gt=0,
        description="How many locations to ask the dataset for per round trip.",
    )
    continue_after_location_id: str | None = Field(
        default=None,
        description="Start streaming after this location id instead of at the beginning.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Send each time series in this attribute instead of as the IP's content.",
    )
    id_attr: str = Field(
        default="id",
        description=(
            "Attribute to carry the location's identifier: 'row-<r>_col-<c>' for a grid dataset, "
            "otherwise the location's own id. Empty adds no attribute."
        ),
    )
    create_substream: bool = Field(
        default=False,
        description="Wrap each dataset's time series in a substream, bracketed by the dataset's id.",
    )
    maintain_incoming_substreams: bool = Field(
        default=False,
        description="Forward incoming bracket IPs. If false, incoming substreams are flattened.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an input that yields no usable dataset is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="climate", name="Climate"),
    info=meta.Info(
        id="ce4749cc-abab-4830-9eb3-1c44c9d451ce",
        name="datasets -> timeseries",
        description=(
            "Stream a capability to every time series of an incoming climate dataset. Accepts a "
            "live capability or a sturdy ref. Locations are fetched in pages, so a large dataset "
            "does not have to be held at once. Incoming substreams are flattened unless "
            "'maintain_incoming_substreams' is set."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="ds",
            contentType="climate.capnp:Dataset",
            desc="Climate dataset, as a capability or a sturdy ref.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="ts",
            contentType=TIMESERIES_TYPE,
            desc="One IP per location of each incoming dataset.",
            required=True,
        ),
    ],
    config=Config,
)


def location_id_of(location: Any) -> str:
    """A grid location's 'row-<r>_col-<c>', or else the location's own id.

    Grid-backed services put a `Geo.RowCol` first in `customData`; other datasets put nothing
    there at all, which used to raise an IndexError and abort the whole dataset. Note that a
    `customData[0]` holding some *other* struct would be misread rather than rejected (D14) -
    Cap'n Proto cannot tell what an AnyPointer was written as.
    """

    if len(location.customData) > 0:
        try:
            row_col = location.customData[0].value.as_struct(geo_capnp.RowCol)
        except capnp.KjException:
            pass
        else:
            return f"row-{row_col.row}_col-{row_col.col}"
    return location.id.id


class DatasetsToTimeseries(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    async def write_timeseries(self, location: Any, in_ip: Any) -> bool:
        """One outgoing IP carrying a location's time series capability."""

        out_ip = fbp_capnp.IP.new_message()
        extra: dict[str, Any] = {}
        if self.config.id_attr:
            extra[self.config.id_attr] = location_id_of(location)
        if self.config.to_attr:
            extra[self.config.to_attr] = brackets.Attr(location.timeSeries, TIMESERIES_TYPE)
        else:
            out_ip.content = location.timeSeries
            out_ip.sysAttributes.contentType = TIMESERIES_TYPE
        brackets.copy_attrs(in_ip, out_ip, extra=extra)
        self.sent += 1
        return await self.write_out("ts", out_ip)

    async def stream_dataset(self, dataset: Any, in_ip: Any) -> bool:
        """Page through a dataset's locations, emitting one IP each. False means stop the process."""

        callback = dataset.streamLocations(self.config.continue_after_location_id or "").locationsCallback
        # Only needed to label the brackets, so an unbracketed run never waits on it.
        info_promise = dataset.info() if self.config.create_substream else None
        opened = False
        dataset_id = ""

        while True:
            locations = (await callback.nextLocations(self.config.no_of_locations_at_once)).locations
            if len(locations) == 0:
                break

            if self.config.create_substream and not opened:
                # Deferred until there is something to put inside, so a dataset without
                # locations does not leave an empty substream behind.
                dataset_id = (await info_promise).id if info_promise is not None else ""
                if not await self.write_out("ts", brackets.make_bracket("openBracket", content=dataset_id)):
                    return False
                opened = True

            for location in locations:
                if not await self.write_timeseries(location, in_ip):
                    return False

        if opened and not await self.write_out("ts", brackets.make_bracket("closeBracket", content=dataset_id)):
            return False
        return True

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["ds"] and self.out_ports["ts"]:
            in_ip = await self.read_in("ds")
            if in_ip is None:
                self.in_ports["ds"] = None
                break

            if brackets.is_bracket(in_ip):
                # The close-bracket branch used to fall through and be treated as a dataset.
                if self.config.maintain_incoming_substreams and not await self.write_out("ts", in_ip):
                    break
                continue

            dataset, _ = await self.cast_cap_or_connect(in_ip.content, climate_capnp.Dataset)
            if dataset is None:
                message = f"{self.name}: no climate dataset could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            if not await self.stream_dataset(dataset, in_ip):
                break

        logger.info("%s process finished, sent %d time series", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(DatasetsToTimeseries(METADATA), METADATA)


if __name__ == "__main__":
    main()
