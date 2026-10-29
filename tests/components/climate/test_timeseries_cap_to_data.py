"""timeseries_cap_to_data, converted from Runnable to Process style (plan LP3).

The Runnable version could never emit anything: see the three fatal faults pinned below.
"""

from __future__ import annotations

import json

import pytest
from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp

from tests.component_harness import (
    cap_message,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from tests.fake_services import FakeTimeSeries
from zalfmas_fbp.components.climate.timeseries_cap_to_data import METADATA, TimeseriesCapToData

ROWS = [[1.0, 10.0, 100.0], [2.0, 20.0, 200.0], [3.0, 30.0, 300.0]]
HEADER = ["tavg", "precip", "globrad"]


def series(**kwargs):
    defaults = {"header": list(HEADER), "data": [list(row) for row in ROWS]}
    return FakeTimeSeries(**(defaults | kwargs))


def run(messages, *, after=None, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(TimeseriesCapToData(METADATA), inputs=inputs, outputs=("out",), after=after)


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def data_of(writer, index=0):
    return standard(writer)[index].content.as_struct(climate_capnp.TimeSeriesData)


def rows_of(tsd):
    return [[round(cell, 3) for cell in row] for row in tsd.data]


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_the_series_data() -> None:
    """The Runnable version emitted nothing at all: `config["subrange_to"]` raised KeyError on
    every IP - the key is `subrange_end` - and the blanket except swallowed it."""

    result = run([cap_message(lambda: series())], subheader=[])
    assert rows_of(data_of(result.output("out"))) == ROWS


def test_the_default_subheader_narrows_the_series() -> None:
    """`config["subheader"].split(",")` was called on a list default, raising AttributeError."""

    result = run([cap_message(lambda: series())])
    tsd = data_of(result.output("out"))
    assert [str(e) for e in tsd.header] == ["tavg", "precip"]
    assert rows_of(tsd) == [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]]


def test_subheader_order_is_the_configured_one() -> None:
    ts = series()
    run([cap_message(lambda: ts)], subheader=["globrad", "tavg"])
    assert ts.subheader_args == [["globrad", "tavg"]]


def test_an_empty_subheader_keeps_every_element() -> None:
    result = run([cap_message(lambda: series())], subheader=[])
    assert [str(e) for e in data_of(result.output("out")).header] == HEADER


def test_transposed_asks_for_the_transposed_data() -> None:
    """`config["transposed"] == "true"` compared a bool to a string, so this was never on."""

    ts = series()
    result = run([cap_message(lambda: ts)], transposed=True, subheader=[])
    tsd = data_of(result.output("out"))
    assert tsd.isTransposed
    assert "dataT" in ts.calls
    assert rows_of(tsd) == [[1.0, 2.0, 3.0], [10.0, 20.0, 30.0], [100.0, 200.0, 300.0]]


def test_untransposed_by_default() -> None:
    ts = series()
    result = run([cap_message(lambda: ts)], subheader=[])
    assert not data_of(result.output("out")).isTransposed
    assert "data" in ts.calls


def test_a_subrange_is_passed_as_capnp_dates() -> None:
    """The dates arrive as ISO strings; the old code handed them straight to `py_date.year`."""

    ts = series()
    run([cap_message(lambda: ts)], subrange_start="2020-03-01", subrange_end="2020-03-31")
    assert ts.subrange_args == [((2020, 3, 1), (2020, 3, 31))]


def test_an_open_ended_subrange_leaves_the_other_bound_unset() -> None:
    ts = series()
    run([cap_message(lambda: ts)], subrange_start="2020-03-01")
    assert ts.subrange_args == [((2020, 3, 1), None)]

    ts = series()
    run([cap_message(lambda: ts)], subrange_end="2020-03-31")
    assert ts.subrange_args == [(None, (2020, 3, 31))]


def test_no_subrange_call_when_neither_bound_is_set() -> None:
    ts = series()
    run([cap_message(lambda: ts)])
    assert ts.subrange_args == []


def test_the_range_and_resolution_come_from_the_series() -> None:
    ts = series(start_date=(2019, 5, 4), end_date=(2019, 6, 7), resolution="hourly")
    result = run([cap_message(lambda: ts)])
    tsd = data_of(result.output("out"))
    assert (tsd.startDate.year, tsd.startDate.month, tsd.startDate.day) == (2019, 5, 4)
    assert (tsd.endDate.year, tsd.endDate.month, tsd.endDate.day) == (2019, 6, 7)
    assert str(tsd.resolution) == "hourly"


def test_outgoing_ips_are_tagged_with_the_data_type() -> None:
    result = run([cap_message(lambda: series())])
    assert standard(result.output("out"))[0].sysAttributes.contentType == "climate.capnp:TimeSeriesData"


def test_to_attr_puts_the_data_in_an_attribute_instead() -> None:
    result = run([cap_message(lambda: series())], to_attr="climate")
    out = standard(result.output("out"))[0]
    assert "climate" in [entry.key for entry in out.attributes]
    assert not out.sysAttributes.contentType


def test_from_attr_reads_the_capability_out_of_an_attribute() -> None:
    result = run([cap_message(lambda: series(), to_attr="ts")], from_attr="ts", subheader=[])
    assert rows_of(data_of(result.output("out"))) == ROWS


def test_a_missing_from_attr_is_skipped() -> None:
    result = run([cap_message(lambda: series(), to_attr="other")], from_attr="ts")
    assert standard(result.output("out")) == []


def test_input_attributes_are_carried_over() -> None:
    result = run([cap_message(lambda: series(), scenario="rcp85")])
    assert "scenario" in [entry.key for entry in standard(result.output("out"))[0].attributes]


def test_incoming_brackets_pass_through() -> None:
    """Transparent by default, like every other Process component."""

    result = run([open_bracket_message(), cap_message(lambda: series()), close_bracket_message()])
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_a_close_bracket_is_not_mistaken_for_a_timeseries() -> None:
    result = run([close_bracket_message(), cap_message(lambda: series())])
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_input_is_skipped_by_default() -> None:
    result = run([ip_message("not a time series"), cap_message(lambda: series())])
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_input_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no time series"):
        run([ip_message("not a time series")], on_error="fail")


def test_several_series_in_a_row() -> None:
    result = run([cap_message(lambda: series()), cap_message(lambda: series())])
    assert len(standard(result.output("out"))) == 2


def test_a_bad_iso_date_is_rejected_without_killing_the_component(caplog) -> None:
    """Pydantic catches it, and the runtime keeps the previous config rather than dying - so the
    series is still processed, just without a subrange."""

    ts = series()
    result = run([cap_message(lambda: ts)], subrange_start="not-a-date")
    assert ts.subrange_args == []
    assert len(standard(result.output("out"))) == 1
    assert "could not apply config" in caplog.text
