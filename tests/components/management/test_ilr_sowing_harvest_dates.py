"""ilr_sowing_harvest_dates, converted from Runnable to Process style (plan LP2).

The Runnable version could not work: it imported ILRDates from management_capnp where it lives in
monica_management_capnp, indexed its data dict with the 0-d numpy array the interpolator returns,
never cleared its input port on 'done' so it spun forever on an exhausted stream, and had no
defaultConfig so every config key raised KeyError. Any one of those was fatal; a bare except turned
all of them into a log line per IP.

Uses the real ILR data under clim4cast, so the date arithmetic is checked against what the
component actually consumes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.model.monica import monica_management_capnp as mgmt_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE
from zalfmas_fbp.components.management.ilr_sowing_harvest_dates import (
    ILR_DATES_TYPE,
    METADATA,
    ILRSowingHarvestDates,
    ilr_date_fields,
)

ILR_DIR = Path("/home/berg/GitHub/clim4cast/data/projects/monica-germany")
WW_CSV = ILR_DIR / "ILR_SEED_HARVEST_doys_WW.csv"

needs_ilr_data = pytest.mark.skipif(not WW_CSV.is_file(), reason=f"ILR data not available at {ILR_DIR}")

# One station's entry, as read_data_and_create_seed_harvest_geo_grid_interpolator produces it.
WW_STATION = {
    "sowing-doy": 271,
    "sowing-date": {"year": 0, "month": 9, "day": 28},
    "earliest-sowing-doy": 260,
    "earliest-sowing-date": {"year": 0, "month": 9, "day": 17},
    "latest-sowing-doy": 288,
    "latest-sowing-date": {"year": 0, "month": 10, "day": 15},
    "harvest-doy": 211,
    "harvest-date": {"year": 0, "month": 7, "day": 30},
    "earliest-harvest-doy": 197,
    "earliest-harvest-date": {"year": 1, "month": 7, "day": 16},
    "latest-harvest-doy": 227,
    "latest-harvest-date": {"year": 1, "month": 8, "day": 15},
}


# --- the date arithmetic, on its own --------------------------------------------------------------


def test_fixed_and_fixed_gives_a_sowing_and_a_harvest() -> None:
    fields = ilr_date_fields(WW_STATION, is_winter_crop=True, sowing_time="fixed", harvest_time="fixed")
    assert set(fields) == {"sowing", "harvest"}
    assert fields["sowing"] == {"year": 0, "month": 9, "day": 28}
    assert fields["harvest"] == {"year": 0, "month": 7, "day": 30}


def test_a_winter_crops_harvest_cannot_run_past_the_day_before_sowing() -> None:
    """Winter crops are harvested the year after sowing, so the harvest doy is clamped."""
    late = {**WW_STATION, "harvest-date": {"year": 0, "month": 12, "day": 31}}
    fields = ilr_date_fields(late, is_winter_crop=True, sowing_time="fixed", harvest_time="fixed")
    assert (fields["harvest"]["month"], fields["harvest"]["day"]) == (9, 27)


def test_a_summer_crops_harvest_is_not_clamped() -> None:
    late = {**WW_STATION, "harvest-date": {"year": 0, "month": 12, "day": 31}}
    fields = ilr_date_fields(late, is_winter_crop=False, sowing_time="fixed", harvest_time="fixed")
    assert (fields["harvest"]["month"], fields["harvest"]["day"]) == (12, 31)


def test_fixed_sowing_with_auto_harvest_gives_a_latest_harvest() -> None:
    fields = ilr_date_fields(WW_STATION, is_winter_crop=True, sowing_time="fixed", harvest_time="auto")
    assert set(fields) == {"sowing", "latestHarvest"}


def test_auto_sowing_with_fixed_harvest_gives_a_sowing_window() -> None:
    fields = ilr_date_fields(WW_STATION, is_winter_crop=True, sowing_time="auto", harvest_time="fixed")
    assert set(fields) == {"earliestSowing", "latestSowing", "harvest"}
    assert fields["earliestSowing"] == WW_STATION["earliest-sowing-date"]


def test_an_early_earliest_sowing_is_pushed_to_june_20() -> None:
    early = {**WW_STATION, "earliest-sowing-date": {"year": 0, "month": 3, "day": 1}}
    fields = ilr_date_fields(early, is_winter_crop=True, sowing_time="auto", harvest_time="fixed")
    assert (fields["earliestSowing"]["month"], fields["earliestSowing"]["day"]) == (6, 20)


def test_auto_and_auto_gives_both_windows() -> None:
    fields = ilr_date_fields(WW_STATION, is_winter_crop=True, sowing_time="auto", harvest_time="auto")
    assert set(fields) == {"earliestSowing", "latestSowing", "latestHarvest"}


def test_every_mode_builds_a_valid_ilr_dates_message() -> None:
    for sowing in ("fixed", "auto"):
        for harvest in ("fixed", "auto"):
            fields = ilr_date_fields(WW_STATION, True, sowing, harvest)  # pyright: ignore[reportArgumentType]
            assert mgmt_capnp.ILRDates.new_message(**fields) is not None


# --- the component, against the real data ---------------------------------------------------------


def coord_ip(lat, lon, crop="WW", sowing="fixed", harvest="fixed"):
    ip = fbp_capnp.IP.new_message(content="payload")
    entries = {"latlon": None, "cropId": crop, "sowingTime": sowing, "harvestTime": harvest}
    kvs = ip.init("attributes", len(entries))
    for i, (key, value) in enumerate(entries.items()):
        kvs[i].key = key
        if key == "latlon":
            kvs[i].value = geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon)
        else:
            kvs[i].value = common_capnp.Value.new_message(t=value)
            kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(ILRSowingHarvestDates(METADATA), inputs=inputs, outputs=("out",)).output()


WW_CONF = {"crop_ids": ["WW"], "path_to_ilr_csv": {"WW": str(WW_CSV)}}


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


@needs_ilr_data
def test_it_attaches_dates_for_a_german_location() -> None:
    """A lat/lon near the Magdeburg station in the real WW data."""
    writer = run([coord_ip(52.6861, 11.4272)], **WW_CONF)
    out = writer.values[0]
    attr = next(kv for kv in out.attributes if kv.key == "ilr")
    dates = attr.value.as_struct(mgmt_capnp.ILRDates)
    assert dates.sowing.month == 9
    assert attr.valueType == ILR_DATES_TYPE


@needs_ilr_data
def test_it_finishes_rather_than_spinning_on_an_exhausted_input() -> None:
    """The Runnable version continued without clearing the port, looping forever."""
    writer = run([coord_ip(52.6861, 11.4272), coord_ip(51.189, 8.4671)], **WW_CONF)
    assert len(writer.values) == 2


@needs_ilr_data
def test_the_dates_can_go_into_the_content_instead() -> None:
    writer = run([coord_ip(52.6861, 11.4272)], **WW_CONF, to_attr=None)
    out = writer.values[0]
    assert out.sysAttributes.contentType == ILR_DATES_TYPE
    assert out.content.as_struct(mgmt_capnp.ILRDates).sowing.month == 9


@needs_ilr_data
def test_auto_modes_produce_windows_end_to_end() -> None:
    writer = run([coord_ip(52.6861, 11.4272, sowing="auto", harvest="auto")], **WW_CONF)
    attr = next(kv for kv in writer.values[0].attributes if kv.key == "ilr")
    dates = attr.value.as_struct(mgmt_capnp.ILRDates)
    assert dates.earliestSowing.month > 0
    assert dates.latestSowing.month > 0


@needs_ilr_data
def test_an_ip_without_the_needed_attributes_is_forwarded_unchanged() -> None:
    writer = run([ip_message("bare")], **WW_CONF)
    assert [v.content.as_text() for v in writer.values] == ["bare"]


@needs_ilr_data
def test_such_an_ip_can_be_dropped_instead() -> None:
    writer = run([ip_message("bare")], **WW_CONF, forward_without_dates=False)
    assert writer.values == []


@needs_ilr_data
def test_an_unknown_crop_forwards_the_ip_without_dates() -> None:
    writer = run([coord_ip(52.6861, 11.4272, crop="XX")], **WW_CONF)
    assert [kv.key for kv in writer.values[0].attributes] == ["latlon", "cropId", "sowingTime", "harvestTime"]


@needs_ilr_data
def test_bracket_ips_pass_through() -> None:
    writer = run(
        [open_bracket_message(), coord_ip(52.6861, 11.4272), close_bracket_message()],
        **WW_CONF,
    )
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_no_loadable_crop_data_stops_the_component() -> None:
    assert run([coord_ip(52.0, 11.0)], crop_ids=["WW"], path_to_ilr_csv={"WW": "/nope.csv"}).values == []
