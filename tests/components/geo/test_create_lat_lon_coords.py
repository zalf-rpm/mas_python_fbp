"""create_lat_lon_coords, converted from Runnable to Process style (plan LP2).

The Runnable version had three independent faults: it called json.loads on a config value that was
already a list (crashing at startup with the metadata defaults), it appended the *builtin* `id`
instead of the cell's id so the JSON path could never serialise, and it indexed the 'earth' bounds
as if they held tl/br directly when they are keyed by resolution.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from mas.schema.common import common_capnp
from mas.schema.geo import geo_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.common.values import python_from_attr
from zalfmas_fbp.components.geo.create_lat_lon_coords import (
    METADATA,
    CreateLatLonCoords,
    bounds_for,
    coordinates_in,
)

# A 2x2 degree box, so a 5min grid over it is small.
SMALL = {"small": {"tl": {"lat": 51.0, "lon": 10.0}, "br": {"lat": 50.9, "lon": 10.1}}}

IDS_GRID = """ncols 2
nrows 2
xllcorner 10.0
yllcorner 50.9
cellsize 0.05
NODATA_value -9999
7 7
8 -9999
"""


def conf(**settings):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings)))


def run(inputs=None, **settings):
    ports: dict = dict(inputs or {})
    if settings:
        ports["conf"] = [conf(**settings), done_message()]
    return run_process_component(CreateLatLonCoords(METADATA), inputs=ports, outputs=("out",)).output()


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


# --- the pure helpers ---------------------------------------------------------------------------


def test_built_in_regions_resolve() -> None:
    assert bounds_for("africa", "5min")["tl"]["lat"] == 37.4


def test_earth_bounds_are_keyed_by_resolution() -> None:
    """The previous version indexed ['tl'] straight off this and raised KeyError."""
    assert bounds_for("earth", "5min")["tl"]["lat"] == pytest.approx(83.958, abs=1e-3)
    assert bounds_for("earth", "30sec")["tl"]["lat"] == pytest.approx(83.996, abs=1e-3)


def test_an_unknown_region_has_no_bounds() -> None:
    assert bounds_for("atlantis", "5min") is None


def test_custom_bounds_take_precedence() -> None:
    assert bounds_for("small", "5min", SMALL)["tl"]["lat"] == 51.0


def test_coordinates_step_by_the_resolution() -> None:
    coords = coordinates_in(SMALL["small"], "5min")
    assert (51.0, 10.0) in coords
    assert all(50.9 <= lat <= 51.0 for lat, _ in coords)


# --- the component ------------------------------------------------------------------------------


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_runs_with_the_metadata_defaults() -> None:
    """The Runnable version crashed at startup on json.loads of the default ids list."""
    defaults = METADATA.default_config_values()
    assert isinstance(defaults["ids"], list)


def test_it_emits_a_json_array_by_default() -> None:
    writer = run(custom_bounds=SMALL, region="small")
    payload = json.loads(standard(writer)[0].content.as_text())
    assert payload
    assert all(len(entry) == 3 for entry in payload)


def test_the_json_array_holds_the_cell_id_not_the_builtin() -> None:
    """The previous version appended the builtin `id`, so json.dumps raised and nothing was sent."""
    writer = run(custom_bounds=SMALL, region="small")
    payload = json.loads(standard(writer)[0].content.as_text())
    assert all(entry[2] is None or isinstance(entry[2], int) for entry in payload)


def test_it_can_stream_one_ip_per_coordinate() -> None:
    writer = run(custom_bounds=SMALL, region="small", stream=True)
    values_out = standard(writer)
    assert len(values_out) > 1
    pair = values_out[0].content.as_struct(common_capnp.Pair)
    coord = pair.snd.as_struct(geo_capnp.LatLonCoord)
    assert 50.9 <= coord.lat <= 51.0


def test_streaming_can_be_wrapped_in_a_substream() -> None:
    writer = run(custom_bounds=SMALL, region="small", stream=True, create_substream=True)
    shapes = [str(v.type) for v in writer.values]
    assert shapes[0] == "openBracket"
    assert shapes[-1] == "closeBracket"


def test_an_id_grid_filters_and_labels_the_cells(tmp_path: Path) -> None:
    path = tmp_path / "ids.asc"
    path.write_text(IDS_GRID)
    writer = run(custom_bounds=SMALL, region="small", path_to_ids_grid=str(path))
    payload = json.loads(standard(writer)[0].content.as_text())
    assert {entry[2] for entry in payload} <= {7, 8}


def test_ids_selects_which_cells_to_keep(tmp_path: Path) -> None:
    path = tmp_path / "ids.asc"
    path.write_text(IDS_GRID)
    writer = run(custom_bounds=SMALL, region="small", path_to_ids_grid=str(path), ids=[8])
    payload = json.loads(standard(writer)[0].content.as_text())
    assert {entry[2] for entry in payload} == {8}


def test_an_unknown_region_emits_nothing_and_says_so() -> None:
    assert standard(run(region="atlantis")) == []


def test_a_missing_id_grid_stops_the_component() -> None:
    assert run(custom_bounds=SMALL, region="small", path_to_ids_grid="/nope/ids.asc").values == []


def test_the_region_port_drives_successive_regions() -> None:
    other = {**SMALL, "tiny": {"tl": {"lat": 50.0, "lon": 10.0}, "br": {"lat": 49.95, "lon": 10.05}}}
    writer = run(
        inputs={"region": [ip_message("small"), ip_message("tiny"), done_message()]},
        custom_bounds=other,
    )
    regions = [python_from_attr(next(kv for kv in v.attributes if kv.key == "region")) for v in standard(writer)]
    assert regions == ["small", "tiny"]


def test_the_result_carries_its_region_and_count() -> None:
    writer = run(custom_bounds=SMALL, region="small")
    attrs = {kv.key: python_from_attr(kv) for kv in standard(writer)[0].attributes}
    assert attrs["region"] == "small"
    assert attrs["count"] > 0
