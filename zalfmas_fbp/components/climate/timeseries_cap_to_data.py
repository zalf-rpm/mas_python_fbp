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
from datetime import date
from typing import Any, Literal, override

from mas.schema.climate import climate_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

TIMESERIES_DATA_TYPE = "climate.capnp:TimeSeriesData"


class Config(process.ProcessConfig):
    from_attr: str | None = Field(
        default=None,
        description="Read the time series capability from this attribute instead of the content.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Write the data to this attribute instead of the IP's content.",
    )
    subrange_start: date | None = Field(
        default=None,
        description="Start of the wanted date range, as an ISO date. Unset means from the beginning.",
    )
    subrange_end: date | None = Field(
        default=None,
        description="End of the wanted date range, as an ISO date. Unset means to the end.",
    )
    subheader: list[str] = Field(
        default_factory=lambda: ["tavg", "precip"],
        description="Climate elements to fetch, in this order. Empty means whatever the series has.",
    )
    transposed: bool = Field(
        default=False,
        description="Fetch the data transposed - one list per element rather than per day.",
    )
    maintain_substreams: bool = Field(
        default=False,
        description="Forward incoming bracket IPs. If false, incoming substreams are flattened.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an input that yields no usable time series is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="climate", name="Climate"),
    info=meta.Info(
        id="b510d603-8f2a-4fbd-ac24-634362b4b0f4",
        name="timeseries capability -> data",
        description=(
            "Fetch the actual data behind a time series capability and send it on as plain "
            "TimeSeriesData. Can narrow the series to a date range and a set of elements first. "
            "Accepts a live capability or a sturdy ref."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="climate.capnp:TimeSeries",
            desc="Time series, as a capability or a sturdy ref.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=TIMESERIES_DATA_TYPE,
            desc="The time series' data, header, range and resolution.",
            required=True,
        ),
    ],
    config=Config,
)


def capnp_date(day: date | None) -> dict[str, int]:
    """A Cap'n Proto date, or an all-zero one, which the schema reads as 'not set'."""

    if day is None:
        return {"year": 0, "month": 0, "day": 0}
    return {"year": day.year, "month": day.month, "day": day.day}


class TimeseriesCapToData(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    def narrow(self, timeseries: Any) -> Any:
        """Apply the configured element and date restrictions, each as one pipelined call."""

        if self.config.subheader:
            timeseries = timeseries.subheader(self.config.subheader).timeSeries
        if self.config.subrange_start is not None or self.config.subrange_end is not None:
            timeseries = timeseries.subrange(
                capnp_date(self.config.subrange_start),
                capnp_date(self.config.subrange_end),
            ).timeSeries
        return timeseries

    async def data_of(self, timeseries: Any) -> Any:
        """Everything the outgoing TimeSeriesData needs, fetched concurrently."""

        # All four are requested before any is awaited, so they cost one round trip, not four.
        header_promise = timeseries.header()
        range_promise = timeseries.range()
        resolution_promise = timeseries.resolution()
        data_promise = timeseries.dataT() if self.config.transposed else timeseries.data()

        header = (await header_promise).header
        rows = (await data_promise).data

        tsd = climate_capnp.TimeSeriesData.new_message()
        tsd.isTransposed = self.config.transposed
        tsd.init("data", len(rows))
        for i, row in enumerate(rows):
            out_row = tsd.data.init(i, len(row))
            for j, cell in enumerate(row):
                out_row[j] = cell

        span = await range_promise
        tsd.startDate = span.startDate
        tsd.endDate = span.endDate
        tsd.resolution = (await resolution_promise).resolution
        out_header = tsd.init("header", len(header))
        for i, element in enumerate(header):
            out_header[i] = element
        return tsd

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if self.config.maintain_substreams and not await self.write_out("out", in_ip):
                    break
                continue

            source = in_ip.content
            if self.config.from_attr:
                kv = values.attr_reader(in_ip, self.config.from_attr)
                if kv is None:
                    logger.warning("%s: no attribute %r on this IP", self.name, self.config.from_attr)
                    continue
                source = kv.value

            timeseries, _ = await self.cast_cap_or_connect(source, climate_capnp.TimeSeries)
            if timeseries is None:
                message = f"{self.name}: no time series could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            tsd = await self.data_of(self.narrow(timeseries))

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if self.config.to_attr:
                extra[self.config.to_attr] = brackets.Attr(tsd, TIMESERIES_DATA_TYPE)
            else:
                out_ip.content = tsd
                out_ip.sysAttributes.contentType = TIMESERIES_DATA_TYPE
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            self.sent += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, sent %d time series", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(TimeseriesCapToData(METADATA), METADATA)


if __name__ == "__main__":
    main()
