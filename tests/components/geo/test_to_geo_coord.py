"""to_geo_coord, converted from Runnable to Process style (plan LP1)."""

from __future__ import annotations

import json

import capnp
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
from zalfmas_fbp.components.common.values import VALUE_TYPE
from zalfmas_fbp.components.geo.to_geo_coord import METADATA, ToGeoCoord


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def capnp_list_ip(numbers, element=None):
    """An IP whose content is a raw Cap'n Proto list, as the Runnable version expected."""
    element = element or capnp.types.Float64
    ip = fbp_capnp.IP.new_message()
    entries = ip.content.init_as_list(capnp._ListSchema(element), len(numbers))  # noqa: SLF001
    for i, number in enumerate(numbers):
        entries[i] = number
    return PortMessage(PortValue(ip))


def value_ip(numbers):
    ip = fbp_capnp.IP.new_message(
        content=common_capnp.Value.new_message(lf64=[float(n) for n in numbers]),
        sysAttributes={"contentType": VALUE_TYPE},
    )
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    inputs: dict = {"vals": [*messages, done_message()]}
    if settings:
        inputs["conf"] = conf(**settings)
    return run_process_component(ToGeoCoord(METADATA), inputs=inputs, outputs=("coord",)).output("coord")


def coords(writer, schema=None):
    schema = schema or geo_capnp.LatLonCoord
    return [v.content.as_struct(schema) for v in writer.values if str(v.type) == "standard"]


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_a_raw_capnp_list_becomes_a_latlon_coord() -> None:
    """The shape the Runnable version accepted, unchanged."""
    writer = run([capnp_list_ip([11.0, 52.0])])
    coord = coords(writer)[0]
    assert (coord.lon, coord.lat) == (11.0, 52.0)


def test_an_integer_list_works_when_configured() -> None:
    writer = run([capnp_list_ip([11, 52], capnp.types.Int64)], list_type="int")
    coord = coords(writer)[0]
    assert (coord.lon, coord.lat) == (11.0, 52.0)


def test_a_common_value_list_is_accepted() -> None:
    """What sequence and json_to_common_value emit, which the Runnable version could not read."""
    coord = coords(run([value_ip([11.0, 52.0])]))[0]
    assert (coord.lon, coord.lat) == (11.0, 52.0)


def test_json_text_is_accepted() -> None:
    coord = coords(run([ip_message(json.dumps([11.0, 52.0]))]))[0]
    assert (coord.lon, coord.lat) == (11.0, 52.0)


def test_other_coordinate_systems_can_be_produced() -> None:
    writer = run([capnp_list_ip([4485000.0, 5763000.0])], to_name="GK5")
    coord = coords(writer, geo_capnp.GKCoord)[0]
    assert (coord.r, coord.h) == (4485000.0, 5763000.0)


def test_the_value_positions_are_configurable() -> None:
    coord = coords(run([ip_message(json.dumps([0, 52.0, 11.0]))], x_index=2, y_index=1))[0]
    assert (coord.lon, coord.lat) == (11.0, 52.0)


def test_too_few_values_skips_the_ip_rather_than_crashing() -> None:
    """The Runnable version raised a ValueError inside its loop for this."""
    writer = run([ip_message(json.dumps([11.0])), ip_message(json.dumps([11.0, 52.0]))])
    assert len(coords(writer)) == 1


def test_unreadable_content_is_skipped() -> None:
    assert coords(run([ip_message("not a list")])) == []


def test_an_unknown_coordinate_name_stops_the_component_with_a_clear_error() -> None:
    """name_to_struct_instance returns None for these, which the Runnable version then crashed on."""
    assert run([capnp_list_ip([11.0, 52.0])], to_name="nonsense").values == []


def test_attributes_are_preserved() -> None:
    ip = fbp_capnp.IP.new_message(content=json.dumps([11.0, 52.0]))
    kvs = ip.init("attributes", 1)
    kvs[0].key = "site"
    kvs[0].value = common_capnp.Value.new_message(t="s1")
    kvs[0].valueType = VALUE_TYPE

    writer = run([PortMessage(PortValue(ip))])
    assert [kv.key for kv in writer.values[0].attributes] == ["site"]


def test_bracket_ips_pass_through() -> None:
    writer = run([open_bracket_message(), ip_message(json.dumps([11.0, 52.0])), close_bracket_message()])
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_every_documented_coordinate_name_works_regardless_of_case() -> None:
    """zalfmas_common.geo lowercases the name for some forms and not others, so the capitalised
    names this component documents - GK5, WGS84, UTM32N - all failed. Normalised here.
    """
    from zalfmas_common import geo

    for name in ("2D", "XY", "LatLon", "WGS84", "GK5", "UTM32N", "gk5", "utm32n"):
        assert geo.name_to_struct_instance(name.lower()) is not None, name


def test_a_utm_coordinate_can_be_produced() -> None:
    writer = run([ip_message(json.dumps([32500000.0, 5763000.0]))], to_name="UTM32N")
    coord = coords(writer, geo_capnp.UTMCoord)[0]
    assert (coord.zone, coord.r, coord.h) == (32, 32500000.0, 5763000.0)
