"""timeseries_data_to_csv, converted from Runnable to Process style (plan LP3)."""

from __future__ import annotations

import json

import pytest
from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.climate.timeseries_data_to_csv import METADATA, TimeseriesDataToCsv

ROWS = [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]]


def tsd(
    *,
    header=("tavg", "precip"),
    data=None,
    transposed=False,
    start=(2020, 1, 1),
    resolution="daily",
):
    message = climate_capnp.TimeSeriesData.new_message()
    rows = [list(r) for r in (ROWS if data is None else data)]
    message.init("data", len(rows))
    for i, row in enumerate(rows):
        out_row = message.data.init(i, len(row))
        for j, cell in enumerate(row):
            out_row[j] = cell
    message.isTransposed = transposed
    out_header = message.init("header", len(header))
    for i, element in enumerate(header):
        out_header[i] = element
    if start is not None:
        message.startDate.year, message.startDate.month, message.startDate.day = start
    message.resolution = resolution
    return message


def data_ip(to_attr=None, **attrs):
    message = tsd() if to_attr is None else None
    ip = fbp_capnp.IP.new_message()
    if to_attr:
        entries = ip.init("attributes", 1 + len(attrs))
        entries[0].key = to_attr
        entries[0].value = tsd()
        for i, (key, value) in enumerate(attrs.items(), start=1):
            entries[i].key = key
            entries[i].value = value
    else:
        ip.content = message
        if attrs:
            entries = ip.init("attributes", len(attrs))
            for i, (key, value) in enumerate(attrs.items()):
                entries[i].key = key
                entries[i].value = value
    return PortMessage(PortValue(ip))


def ip_of(message):
    ip = fbp_capnp.IP.new_message()
    ip.content = message
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(TimeseriesDataToCsv(METADATA), inputs=inputs, outputs=("out",))


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def csv_of(result, index=0) -> str:
    return standard(result.output("out"))[index].content.as_text()


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_a_csv_at_all() -> None:
    """The Runnable version emitted nothing: METADATA carried no defaultConfig, so
    `config["from_attr"]` raised KeyError on every IP and the blanket except swallowed it."""

    assert csv_of(run([data_ip()])) == (
        "date,tavg,precip\n2020-01-01,1.0,10.0\n2020-01-02,2.0,20.0\n2020-01-03,3.0,30.0\n"
    )


def test_the_header_has_as_many_columns_as_the_rows() -> None:
    """The date column used to be missing from the header, leaving it one short of every row."""

    lines = csv_of(run([data_ip()])).strip().split("\n")
    widths = {len(line.split(",")) for line in lines}
    assert widths == {3}


def test_transposed_data_is_turned_back_the_right_way_round() -> None:
    """`isTransposed` was ignored, so this produced one row per element, each with its own date."""

    message = tsd(data=[[1.0, 2.0, 3.0], [10.0, 20.0, 30.0]], transposed=True)
    assert csv_of(run([ip_of(message)])) == (
        "date,tavg,precip\n2020-01-01,1.0,10.0\n2020-01-02,2.0,20.0\n2020-01-03,3.0,30.0\n"
    )


def test_hourly_data_is_stepped_by_the_hour() -> None:
    """Dates advanced a day per row whatever the resolution said."""

    lines = csv_of(run([ip_of(tsd(resolution="hourly"))])).strip().split("\n")
    assert lines[1].split(",")[0] == "2020-01-01T00:00"
    assert lines[2].split(",")[0] == "2020-01-01T01:00"


def test_the_date_format_can_be_set() -> None:
    lines = csv_of(run([data_ip()], date_format="%d.%m.%Y")).strip().split("\n")
    assert lines[1].split(",")[0] == "01.01.2020"


def test_the_date_column_can_be_renamed() -> None:
    assert csv_of(run([data_ip()], date_column="iso_date")).startswith("iso_date,tavg,precip\n")


def test_an_empty_date_column_leaves_the_dates_out() -> None:
    assert csv_of(run([data_ip()], date_column="")) == "tavg,precip\n1.0,10.0\n2.0,20.0\n3.0,30.0\n"


def test_data_without_a_start_date_still_renders() -> None:
    """An all-zero date is 'not set'; `date(year=0, ...)` used to raise and lose the whole IP."""

    assert csv_of(run([ip_of(tsd(start=None))])) == "tavg,precip\n1.0,10.0\n2.0,20.0\n3.0,30.0\n"


def test_the_header_can_be_left_out() -> None:
    assert csv_of(run([data_ip()], include_header=False)).startswith("2020-01-01,")


def test_the_delimiter_can_be_changed() -> None:
    assert csv_of(run([data_ip()], delimiter=";")).startswith("date;tavg;precip\n")


def test_empty_data_yields_just_the_header() -> None:
    assert csv_of(run([ip_of(tsd(data=[]))])) == "date,tavg,precip\n"


def test_to_attr_puts_the_csv_in_an_attribute_instead() -> None:
    result = run([data_ip()], to_attr="csv")
    out = standard(result.output("out"))[0]
    assert "csv" in [entry.key for entry in out.attributes]


def test_from_attr_reads_the_data_out_of_an_attribute() -> None:
    result = run([data_ip(to_attr="climate")], from_attr="climate")
    assert csv_of(result).startswith("date,tavg,precip\n")


def test_a_missing_from_attr_is_skipped() -> None:
    result = run([data_ip(to_attr="other")], from_attr="climate")
    assert standard(result.output("out")) == []


def test_input_attributes_are_carried_over() -> None:
    result = run([data_ip(scenario="rcp85")])
    assert "scenario" in [entry.key for entry in standard(result.output("out"))[0].attributes]


def test_brackets_pass_through_instead_of_being_read_as_data() -> None:
    """A bracket has no content, and `as_struct` cannot refuse it (D14) - it used to be rendered."""

    result = run([open_bracket_message(), data_ip(), close_bracket_message()])
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_an_unreadable_input_is_skipped_by_default() -> None:
    result = run([ip_message("not time series data"), data_ip()])
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_input_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no time series data"):
        run([ip_message("not time series data")], on_error="fail")


def test_several_ips_in_a_row() -> None:
    result = run([data_ip(), data_ip()])
    assert len(standard(result.output("out"))) == 2
