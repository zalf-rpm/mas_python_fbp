"""In-process fakes for the Cap'n Proto services that components talk to.

A component which takes a service over a port cannot be tested with plain IPs: it calls methods
on a capability and reacts to what comes back. These fakes are real `capnp` servers, so the
component exercises the actual RPC path - `as_interface`, method calls, promise pipelining - with
no socket, no process and no network.

Two rules come out of how pycapnp works, and both are load-bearing:

- **Build them inside the event loop.** Attaching a server to a message needs a running kj loop,
  so a fake constructed while a test collects its inputs raises "no running event loop". Pass the
  *class* (or a lambda) to `component_harness.cap_message`, which builds it at read time.
- **Fill `_context.results`, do not return a dict.** For `info @0 () -> IdInformation` the results
  struct *is* the `IdInformation`, so its fields are set directly on `_context.results`.

Every fake records what it was asked for - `calls`, and fields like `requested_latlon` - so a test
can assert that a component asked the right question, not merely that it produced some output.
"""

from __future__ import annotations

from typing import Any

from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp
from mas.schema.geo import geo_capnp


class FakeTimeSeries(climate_capnp.TimeSeries.Server):
    """A time series over a fixed set of elements and rows.

    `data` is row-major - one inner list per day, in `header` order - which is what the schema's
    `data()` returns; `dataT()` transposes it here, exactly as a real implementation would.
    """

    def __init__(
        self,
        *,
        id_: str = "ts-1",
        name: str = "Fake time series",
        header: list[str] | None = None,
        data: list[list[float]] | None = None,
        start_date: tuple[int, int, int] = (2020, 1, 1),
        end_date: tuple[int, int, int] = (2020, 1, 3),
        resolution: str = "daily",
        location_id: str = "loc-1",
        latlon: tuple[float, float] = (52.0, 13.0),
    ):
        self.id = id_
        self.name = name
        self.header_elements = header if header is not None else ["tavg", "precip"]
        self.rows = data if data is not None else [[1.0, 0.0], [2.0, 0.5], [3.0, 1.5]]
        self.start_date = start_date
        self.end_date = end_date
        self.resolution_name = resolution
        self.location_id = location_id
        self.latlon = latlon
        self.calls: list[str] = []
        self.subrange_args: list[tuple[Any, Any]] = []
        self.subheader_args: list[list[str]] = []

    async def info(self, _context, **kwargs):
        self.calls.append("info")
        _context.results.id = self.id
        _context.results.name = self.name

    async def resolution(self, _context, **kwargs):
        self.calls.append("resolution")
        _context.results.resolution = self.resolution_name

    async def range(self, _context, **kwargs):
        self.calls.append("range")
        for field, (year, month, day) in (
            ("startDate", self.start_date),
            ("endDate", self.end_date),
        ):
            date = getattr(_context.results, field)
            date.year = year
            date.month = month
            date.day = day

    async def header(self, _context, **kwargs):
        self.calls.append("header")
        _context.results.header = self.header_elements

    async def data(self, _context, **kwargs):
        self.calls.append("data")
        _context.results.data = self.rows

    async def dataT(self, _context, **kwargs):  # noqa: N802 - the schema's method name
        self.calls.append("dataT")
        _context.results.data = [list(column) for column in zip(*self.rows, strict=True)]

    async def subrange(self, start, end, _context, **kwargs):
        """Records the range it was asked for and hands back a series tagged with it.

        A zero year means 'not set', which is how the schema says an open-ended bound is passed.
        """

        self.calls.append("subrange")
        as_tuple = lambda d: None if d.year == 0 else (d.year, d.month, d.day)  # noqa: E731
        wanted = (as_tuple(start), as_tuple(end))
        self.subrange_args.append(wanted)
        narrowed = self._clone()
        narrowed.start_date = wanted[0] if wanted[0] is not None else self.start_date
        narrowed.end_date = wanted[1] if wanted[1] is not None else self.end_date
        _context.results.timeSeries = narrowed

    async def subheader(self, elements, _context, **kwargs):
        """Really narrows the series, so a component that ignores the result cannot pass."""

        self.calls.append("subheader")
        wanted = [str(e) for e in elements]
        self.subheader_args.append(wanted)
        keep = [self.header_elements.index(e) for e in wanted if e in self.header_elements]
        narrowed = self._clone()
        narrowed.header_elements = [self.header_elements[i] for i in keep]
        narrowed.rows = [[row[i] for i in keep] for row in self.rows]
        _context.results.timeSeries = narrowed

    def _clone(self) -> FakeTimeSeries:
        """A copy sharing this fake's call log, so a test sees the whole conversation in one place."""

        clone = FakeTimeSeries(
            id_=self.id,
            name=self.name,
            header=list(self.header_elements),
            data=[list(row) for row in self.rows],
            start_date=self.start_date,
            end_date=self.end_date,
            resolution=self.resolution_name,
            location_id=self.location_id,
            latlon=self.latlon,
        )
        clone.calls = self.calls
        clone.subrange_args = self.subrange_args
        clone.subheader_args = self.subheader_args
        return clone

    async def metadata(self, _context, **kwargs):
        self.calls.append("metadata")
        _context.results.entries = []

    async def location(self, _context, **kwargs):
        self.calls.append("location")
        _context.results.id.id = self.location_id
        _context.results.heightNN = 0.0
        _context.results.latlon.lat = self.latlon[0]
        _context.results.latlon.lon = self.latlon[1]


class FakeLocation:
    """One climate location: an id, a time series, and optionally a grid row/col in customData.

    Grid-backed climate services put a `Geo.RowCol` in `customData[0]`, which is what components
    reading a grid dataset look for. Set `row_col=None` for a location without one, which is how
    a non-grid dataset looks - components have to cope with both.
    """

    def __init__(
        self,
        *,
        id_: str = "loc-1",
        row_col: tuple[int, int] | None = (0, 0),
        latlon: tuple[float, float] = (52.0, 13.0),
        time_series: FakeTimeSeries | None = None,
        custom_key: str = "rowCol",
    ):
        self.id = id_
        self.row_col = row_col
        self.latlon = latlon
        self.time_series = time_series if time_series is not None else FakeTimeSeries(id_=f"ts-{id_}")
        self.custom_key = custom_key

    def write_into(self, builder) -> None:
        builder.id.id = self.id
        builder.id.name = self.id
        builder.heightNN = 0.0
        builder.latlon.lat = self.latlon[0]
        builder.latlon.lon = self.latlon[1]
        builder.timeSeries = self.time_series
        if self.row_col is not None:
            entries = builder.init("customData", 1)
            entries[0].key = self.custom_key
            row_col = entries[0].value.as_struct(geo_capnp.RowCol)
            row_col.row = self.row_col[0]
            row_col.col = self.row_col[1]


class FakeLocationsCallback(climate_capnp.Dataset.GetLocationsCallback.Server):
    """Hands out locations in pages, then an empty page to signal the end, as the schema expects."""

    def __init__(self, locations: list[FakeLocation]):
        self.remaining = list(locations)
        self.requested_counts: list[int] = []

    async def nextLocations(self, maxCount, _context, **kwargs):  # noqa: N802, N803 - schema names
        self.requested_counts.append(maxCount)
        page = self.remaining[:maxCount]
        self.remaining = self.remaining[maxCount:]
        entries = _context.results.init("locations", len(page))
        for entry, location in zip(entries, page, strict=True):
            location.write_into(entry)


class FakeDataset(climate_capnp.Dataset.Server):
    """A dataset handing out one time series, whichever way it is asked for."""

    def __init__(
        self,
        *,
        id_: str = "ds-1",
        name: str = "Fake dataset",
        time_series: FakeTimeSeries | None = None,
        locations: list[FakeLocation] | None = None,
    ):
        self.id = id_
        self.name = name
        self.time_series = time_series if time_series is not None else FakeTimeSeries()
        self.locations_list = (
            locations
            if locations is not None
            else [FakeLocation(id_="loc-1", row_col=(0, 0)), FakeLocation(id_="loc-2", row_col=(0, 1))]
        )
        self.calls: list[str] = []
        self.requested_latlon: list[tuple[float, float]] = []
        self.requested_location_ids: list[str] = []
        self.stream_started_after: list[str] = []
        self.callbacks: list[FakeLocationsCallback] = []

    async def info(self, _context, **kwargs):
        self.calls.append("info")
        _context.results.id = self.id
        _context.results.name = self.name

    async def metadata(self, _context, **kwargs):
        self.calls.append("metadata")
        _context.results.entries = []

    async def closestTimeSeriesAt(self, latlon, _context, **kwargs):  # noqa: N802 - schema name
        self.calls.append("closestTimeSeriesAt")
        self.requested_latlon.append((latlon.lat, latlon.lon))
        _context.results.timeSeries = self.time_series

    async def timeSeriesAt(self, locationId, _context, **kwargs):  # noqa: N802, N803 - schema names
        self.calls.append("timeSeriesAt")
        self.requested_location_ids.append(locationId)
        _context.results.timeSeries = self.time_series

    async def locations(self, _context, **kwargs):
        self.calls.append("locations")
        entries = _context.results.init("locations", len(self.locations_list))
        for entry, location in zip(entries, self.locations_list, strict=True):
            location.write_into(entry)

    async def streamLocations(self, startAfterLocationId, _context, **kwargs):  # noqa: N802, N803 - schema names
        self.calls.append("streamLocations")
        self.stream_started_after.append(startAfterLocationId)
        remaining = self.locations_list
        if startAfterLocationId:
            ids = [location.id for location in self.locations_list]
            if startAfterLocationId in ids:
                remaining = self.locations_list[ids.index(startAfterLocationId) + 1 :]
        callback = FakeLocationsCallback(remaining)
        self.callbacks.append(callback)
        _context.results.locationsCallback = callback


class FakeClimateService(climate_capnp.Service.Server):
    """A climate service offering a fixed list of datasets."""

    def __init__(
        self,
        *,
        id_: str = "svc-1",
        name: str = "Fake climate service",
        datasets: list[FakeDataset] | None = None,
    ):
        self.id = id_
        self.name = name
        self.datasets = datasets if datasets is not None else [FakeDataset()]
        self.calls: list[str] = []

    async def info(self, _context, **kwargs):
        self.calls.append("info")
        _context.results.id = self.id
        _context.results.name = self.name

    async def getAvailableDatasets(self, _context, **kwargs):  # noqa: N802 - schema name
        self.calls.append("getAvailableDatasets")
        entries = _context.results.init("datasets", len(self.datasets))
        for entry, dataset in zip(entries, self.datasets, strict=True):
            entry.data = dataset

    async def getDatasetsFor(self, template, _context, **kwargs):  # noqa: N802 - schema name
        self.calls.append("getDatasetsFor")
        _context.results.datasets = list(self.datasets)


class FakeIdentifiable(common_capnp.Identifiable.Server):
    """The smallest possible capability, for tests that only need *some* live capability."""

    def __init__(self, *, id_: str = "id-1", name: str = "Fake", description: str = ""):
        self.id = id_
        self.name = name
        self.description = description
        self.calls: list[str] = []

    async def info(self, _context, **kwargs):
        self.calls.append("info")
        _context.results.id = self.id
        _context.results.name = self.name
        _context.results.description = self.description
