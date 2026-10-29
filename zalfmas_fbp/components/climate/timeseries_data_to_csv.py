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

import io
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, override

from mas.schema.climate import climate_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

TIMESERIES_DATA_TYPE = "climate.capnp:TimeSeriesData"
DATE_FORMATS = {"daily": "%Y-%m-%d", "hourly": "%Y-%m-%dT%H:%M"}


class Config(process.ProcessConfig):
    from_attr: str | None = Field(
        default=None,
        description="Read the time series data from this attribute instead of the content.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Write the CSV to this attribute instead of the IP's content.",
    )
    date_column: str = Field(
        default="date",
        description="Header for the leading date column. Empty leaves the dates out entirely.",
    )
    date_format: str = Field(
        default="",
        description=(
            "strftime format for the dates. Empty picks one from the data's resolution: "
            "'%Y-%m-%d' for daily, '%Y-%m-%dT%H:%M' for hourly."
        ),
    )
    delimiter: str = Field(default=",", description="Column separator.")
    include_header: bool = Field(default=True, description="Write the header row.")
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP holding no readable time series data is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="climate", name="Climate"),
    info=meta.Info(
        id="6b11cf2a-08bb-43f9-964a-1d4ed248cce9",
        name="timeseries data -> csv",
        description=(
            "Render plain TimeSeriesData as a CSV string, one row per date. Transposed data is "
            "turned back the right way round first, and hourly data is stepped by the hour. "
            "Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=TIMESERIES_DATA_TYPE,
            desc="The time series data to render.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="The data as a CSV string.",
            required=True,
        ),
    ],
    config=Config,
)


def rows_of(data: Any) -> list[list[float]]:
    """The data as one row per date, transposing it back if it arrived the other way round.

    `dataT` gives one list per element, which is the transpose of what a CSV needs. The old code
    ignored `isTransposed` and wrote whichever it got, so transposed input produced a file with
    one row per element, each stamped with a consecutive date.
    """

    rows = [list(row) for row in data.data]
    if data.isTransposed and rows:
        return [list(row) for row in zip(*rows, strict=True)]
    return rows


def start_datetime_of(data: Any) -> datetime | None:
    """The first date, or None if the data carries none - an all-zero date is 'not set'."""

    start = data.startDate
    if start.year == 0 or start.month == 0 or start.day == 0:
        return None
    try:
        return datetime(year=start.year, month=start.month, day=start.day)  # noqa: DTZ001 - a plain calendar date
    except ValueError:
        return None


def data_to_csv(data: Any, config: Config) -> str:
    """One row per date, with the dates stepped by the data's own resolution."""

    resolution = str(data.resolution)
    step = timedelta(hours=1) if resolution == "hourly" else timedelta(days=1)
    date_format = config.date_format or DATE_FORMATS.get(resolution, DATE_FORMATS["daily"])
    start = start_datetime_of(data)
    with_dates = bool(config.date_column) and start is not None

    buffer = io.StringIO()
    if config.include_header:
        # The date column has to be named, or the header is one column short of every row.
        names = ([config.date_column] if with_dates else []) + [str(h) for h in data.header]
        buffer.write(config.delimiter.join(names) + "\n")

    for i, row in enumerate(rows_of(data)):
        cells = [str(cell) for cell in row]
        if with_dates:
            cells.insert(0, (start + i * step).strftime(date_format))  # pyright: ignore[reportOptionalOperand]
        buffer.write(config.delimiter.join(cells) + "\n")
    return buffer.getvalue()


class TimeseriesDataToCsv(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    def data_of(self, in_ip: IPReader) -> Any | None:
        """The incoming TimeSeriesData, from an attribute or the content."""

        if self.config.from_attr:
            kv = values.attr_reader(in_ip, self.config.from_attr)
            if kv is None:
                return None
            pointer = kv.value
        else:
            pointer = in_ip.content
        try:
            return pointer.as_struct(climate_capnp.TimeSeriesData)
        except Exception:  # noqa: BLE001 - as_struct raises several unrelated types
            return None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            # A bracket used to be read as TimeSeriesData, which Cap'n Proto cannot refuse (D14).
            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            data = self.data_of(in_ip)
            if data is None:
                message = f"{self.name}: no time series data could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            csv = data_to_csv(data, self.config)

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if self.config.to_attr:
                extra[self.config.to_attr] = csv
            else:
                out_ip.content = csv
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            self.sent += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, sent %d CSV string(s)", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(TimeseriesDataToCsv(METADATA), METADATA)


if __name__ == "__main__":
    main()
