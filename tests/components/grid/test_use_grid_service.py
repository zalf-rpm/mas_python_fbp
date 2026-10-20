"""use_grid_service, converted from Runnable to Process style (plan LP3).

The Runnable version could not process a single IP: it called `.wait()` on an async read and cast
the service to `grid_capnp.Service`, which does not exist.
"""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.grid import grid_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    cap_message,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from tests.fake_services import FakeGrid
from zalfmas_fbp.components.grid.use_grid_service import METADATA, UseGridService


def coord_ip(lat=52.0, lon=13.0, to_attr=None, **attrs):
    ip = fbp_capnp.IP.new_message()
    coord = geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon)
    named = dict(attrs)
    if to_attr:
        named[to_attr] = coord
    else:
        ip.content = coord
    if named:
        entries = ip.init("attributes", len(named))
        for i, (key, value) in enumerate(named.items()):
            entries[i].key = key
            entries[i].value = value
    return PortMessage(PortValue(ip))


def run(messages, *, grid=None, service_messages=None, **settings):
    made = grid if grid is not None else FakeGrid()
    inputs: dict = {
        "in": [*messages, done_message()],
        "service": service_messages if service_messages is not None else [cap_message(lambda: made), done_message()],
    }
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    result = run_process_component(UseGridService(METADATA), inputs=inputs, outputs=("out",))
    return result, made


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def grid_values(result):
    return [v.content.as_struct(grid_capnp.Grid.Value) for v in standard(result.output("out"))]


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_a_value_at_all() -> None:
    """`in_ports["in"].read().wait()` raised AttributeError, and the try wrapped the whole loop,
    so the component stopped before its first IP. It also cast to a nonexistent interface."""

    result, _ = run([coord_ip()])
    assert [v.f for v in grid_values(result)] == [42.0]


def test_it_asks_the_grid_for_the_incoming_coordinate() -> None:
    _, grid = run([coord_ip(lat=51.5, lon=12.25)])
    assert grid.requested_coords == [(51.5, 12.25)]


def test_outgoing_ips_are_tagged_with_the_grid_value_type() -> None:
    result, _ = run([coord_ip()])
    assert standard(result.output("out"))[0].sysAttributes.contentType == "grid.capnp:Grid.Value"


def test_as_common_value_sends_a_common_value_instead() -> None:
    result, _ = run([coord_ip()], as_common_value=True)
    out = standard(result.output("out"))[0]
    assert out.content.as_struct(common_capnp.Value).f64 == 42.0
    assert out.sysAttributes.contentType.startswith("@0x")


def test_an_integer_grid_keeps_its_union_arm() -> None:
    result, _ = run([coord_ip()], grid=FakeGrid(default=7, value_field="i"))
    assert grid_values(result)[0].which() == "i"
    assert grid_values(result)[0].i == 7


def test_calc_applies_an_expression_to_the_value() -> None:
    """The old code passed `{name, value}` - a set literal, not a dict - as the variable table,
    and read the variable name out of the expression slot."""

    result, _ = run([coord_ip()], calc="v*0.1")
    assert round(grid_values(result)[0].f, 6) == 4.2


def test_calc_can_use_extra_constants() -> None:
    result, _ = run([coord_ip()], calc="v*a+b", calc_constants={"a": 2.0, "b": 1.0})
    assert round(grid_values(result)[0].f, 6) == 85.0


def test_the_calc_variable_can_be_renamed() -> None:
    result, _ = run([coord_ip()], calc="x*2", calc_variable="x")
    assert round(grid_values(result)[0].f, 6) == 84.0


def test_a_multi_letter_calc_variable_is_rejected(caplog) -> None:
    """The documented default used to be 'gv', which this parser silently evaluates to 0."""

    result, _ = run([coord_ip()], calc="gv*2", calc_variable="gv")
    assert "could not apply config" in caplog.text
    assert round(grid_values(result)[0].f, 6) == 42.0


def test_multi_letter_calc_constants_are_rejected(caplog) -> None:
    run([coord_ip()], calc="v*off", calc_constants={"off": 2.0})
    assert "could not apply config" in caplog.text


def test_no_calc_leaves_the_value_untouched() -> None:
    result, _ = run([coord_ip()])
    assert grid_values(result)[0].f == 42.0


def test_a_no_data_cell_is_emitted_by_default() -> None:
    result, _ = run([coord_ip()], grid=FakeGrid(default=None))
    assert grid_values(result)[0].which() == "no"


def test_a_no_data_cell_can_be_skipped() -> None:
    result, _ = run([coord_ip()], grid=FakeGrid(default=None), on_no_data="skip")
    assert standard(result.output("out")) == []


def test_a_no_data_cell_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no data"):
        run([coord_ip()], grid=FakeGrid(default=None), on_no_data="fail")


def test_a_no_data_cell_is_not_put_through_calc() -> None:
    result, _ = run([coord_ip()], grid=FakeGrid(default=None), calc="v*2")
    assert grid_values(result)[0].which() == "no"


def test_ignore_no_data_is_passed_to_the_grid() -> None:
    _, grid = run([coord_ip()])
    assert grid.ignore_no_data_flags == [True]

    _, grid = run([coord_ip()], ignore_no_data=False)
    assert grid.ignore_no_data_flags == [False]


def test_from_attr_reads_the_coordinate_out_of_an_attribute() -> None:
    result, grid = run([coord_ip(lat=50.0, lon=10.0, to_attr="coord")], from_attr="coord")
    assert grid.requested_coords == [(50.0, 10.0)]
    assert len(standard(result.output("out"))) == 1


def test_to_attr_puts_the_value_in_an_attribute_instead() -> None:
    result, _ = run([coord_ip()], to_attr="gridval")
    out = standard(result.output("out"))[0]
    assert "gridval" in [entry.key for entry in out.attributes]
    assert not out.sysAttributes.contentType


def test_input_attributes_are_carried_over() -> None:
    result, _ = run([coord_ip(region="north")])
    assert "region" in [entry.key for entry in standard(result.output("out"))[0].attributes]


def test_brackets_pass_through() -> None:
    result, _ = run([open_bracket_message(), coord_ip(), close_bracket_message()])
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_an_unreadable_coordinate_is_skipped_by_default() -> None:
    result, _ = run([ip_message("not a coordinate"), coord_ip()])
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_coordinate_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no coordinate"):
        run([ip_message("not a coordinate")], on_error="fail")


def test_it_stops_cleanly_when_no_service_arrives() -> None:
    result, _ = run([coord_ip()], service_messages=[done_message()])
    assert result.output("out").values == []


def test_it_stops_cleanly_when_the_service_is_not_a_grid() -> None:
    result, _ = run([coord_ip()], service_messages=[ip_message("not a grid"), done_message()])
    assert result.output("out").values == []


def test_the_service_is_read_once_for_the_whole_run() -> None:
    result, grid = run([coord_ip(), coord_ip(lat=53.0), coord_ip(lat=54.0)])
    assert len(standard(result.output("out"))) == 3
    assert len(grid.requested_coords) == 3
