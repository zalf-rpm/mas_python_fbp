"""get_lat_lon_grid_value, converted from Runnable to Process style (plan LP2).

The Runnable version never emitted anything: it called the grid's value() with two arguments where
three are required, and the resulting TypeError was swallowed by a bare except, once per IP.
"""

from __future__ import annotations

import json
from pathlib import Path

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr, python_from_value
from zalfmas_fbp.components.geo._coord_types import CONTENT_TYPES
from zalfmas_fbp.components.geo.get_lat_lon_grid_value import METADATA, GetLatLonGridValue

LATLON_TYPE = CONTENT_TYPES[geo_capnp.LatLonCoord.schema.node.id]

# 3x2 cells of 1 degree, lower-left at (10, 51), so rows run 52..51 north to south.
GRID = """ncols 3
nrows 2
xllcorner 10.0
yllcorner 51.0
cellsize 1.0
NODATA_value -9999
1 2 3
4 5 -9999
"""


def write_grid(tmp_path: Path, name: str = "grid.asc") -> str:
    path = tmp_path / name
    path.write_text(GRID)
    return str(path)


def coord_ip(lat, lon, **attrs):
    ip = fbp_capnp.IP.new_message(
        content=geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon),
        sysAttributes={"contentType": LATLON_TYPE},
    )
    if attrs:
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
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
    return run_process_component(GetLatLonGridValue(METADATA), inputs=inputs, outputs=("out",)).output()


def emitted(writer):
    return [
        python_from_value(v.content.as_struct(common_capnp.Value)) for v in writer.values if str(v.type) == "standard"
    ]


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_the_grid_value_at_a_coordinate(tmp_path: Path) -> None:
    """The Runnable version emitted nothing here, for every input."""
    writer = run([coord_ip(51.5, 10.5)], path_to_grid=write_grid(tmp_path), type="int")
    assert emitted(writer) == [4]


def test_it_reads_several_cells(tmp_path: Path) -> None:
    grid = write_grid(tmp_path)
    writer = run(
        [coord_ip(52.5, 10.5), coord_ip(52.5, 12.5), coord_ip(51.5, 11.5)],
        path_to_grid=grid,
        type="int",
    )
    assert emitted(writer) == [1, 3, 5]


def test_values_can_be_read_as_floats(tmp_path: Path) -> None:
    writer = run([coord_ip(51.5, 10.5)], path_to_grid=write_grid(tmp_path), type="float")
    assert emitted(writer) == [4.0]


def test_the_output_is_tagged_as_a_common_value(tmp_path: Path) -> None:
    writer = run([coord_ip(51.5, 10.5)], path_to_grid=write_grid(tmp_path))
    assert writer.values[0].sysAttributes.contentType == VALUE_TYPE


def test_a_nodata_cell_is_skipped_by_default(tmp_path: Path) -> None:
    writer = run([coord_ip(51.5, 12.5)], path_to_grid=write_grid(tmp_path), type="int")
    assert emitted(writer) == []


def test_a_coordinate_outside_the_grid_is_skipped(tmp_path: Path) -> None:
    writer = run([coord_ip(10.0, 10.0)], path_to_grid=write_grid(tmp_path), type="int")
    assert emitted(writer) == []


def test_nodata_can_be_emitted_instead(tmp_path: Path) -> None:
    writer = run(
        [coord_ip(51.5, 12.5)],
        path_to_grid=write_grid(tmp_path),
        type="int",
        return_no_data=True,
    )
    assert emitted(writer) == [-9999]


def test_the_value_can_go_to_an_attribute_keeping_the_coordinate(tmp_path: Path) -> None:
    writer = run([coord_ip(51.5, 10.5)], path_to_grid=write_grid(tmp_path), type="int", to_attr="elevation")
    out = writer.values[0]
    assert out.content.as_struct(geo_capnp.LatLonCoord).lat == 51.5
    attr = next(kv for kv in out.attributes if kv.key == "elevation")
    assert python_from_attr(attr) == 4


def test_no_grid_configured_stops_the_component(tmp_path: Path) -> None:
    assert run([coord_ip(51.5, 10.5)]).values == []


def test_a_missing_grid_file_stops_the_component(tmp_path: Path) -> None:
    assert run([coord_ip(51.5, 10.5)], path_to_grid=str(tmp_path / "nope.asc")).values == []


def test_an_unreadable_coordinate_is_skipped(tmp_path: Path) -> None:
    writer = run([ip_message("not a coordinate")], path_to_grid=write_grid(tmp_path))
    assert emitted(writer) == []


def test_attributes_are_preserved(tmp_path: Path) -> None:
    writer = run([coord_ip(51.5, 10.5, site="s1")], path_to_grid=write_grid(tmp_path), type="int")
    assert [kv.key for kv in writer.values[0].attributes] == ["site"]


def test_bracket_ips_pass_through(tmp_path: Path) -> None:
    writer = run(
        [open_bracket_message(), coord_ip(51.5, 10.5), close_bracket_message()],
        path_to_grid=write_grid(tmp_path),
    )
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]
