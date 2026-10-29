"""proj_transform_coordinates, converted from Runnable to Process style (plan LP2)."""

from __future__ import annotations

import json

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
from zalfmas_fbp.components.geo._coord_types import CONTENT_TYPES
from zalfmas_fbp.components.geo.proj_transform_coordinates import METADATA, ProjTransformCoordinates

LATLON_TYPE = CONTENT_TYPES[geo_capnp.LatLonCoord.schema.node.id]
UTM_TYPE = CONTENT_TYPES[geo_capnp.UTMCoord.schema.node.id]


def latlon_ip(lat=52.0, lon=11.0, tagged=True, **attrs):
    ip = fbp_capnp.IP.new_message(content=geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon))
    if tagged:
        ip.sysAttributes.contentType = LATLON_TYPE
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
    return run_process_component(ProjTransformCoordinates(METADATA), inputs=inputs, outputs=("out",)).output()


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_transforms_latlon_to_utm() -> None:
    writer = run([latlon_ip()], from_name="LatLon", to_name="utm32n")
    coord = standard(writer)[0].content.as_struct(geo_capnp.UTMCoord)
    assert coord.zone == 32
    assert round(coord.r) == 637294
    assert round(coord.h) == 5762927


def test_capitalised_crs_names_work() -> None:
    """name_to_struct_type compares the raw string for utm*/gk*/wgs84, so these used to fail."""
    writer = run([latlon_ip()], from_name="WGS84", to_name="UTM32N")
    assert len(standard(writer)) == 1


def test_the_result_is_tagged_with_its_type() -> None:
    """So a type-driven component downstream can read it without being told the type again."""
    writer = run([latlon_ip()], to_name="utm32n")
    assert standard(writer)[0].sysAttributes.contentType == UTM_TYPE


def test_the_ips_declared_type_is_used_over_the_configured_one() -> None:
    """A tagged IP carries its own source CRS, so from_name does not have to be right."""
    writer = run([latlon_ip()], from_name="gk5", to_name="utm32n")
    coord = standard(writer)[0].content.as_struct(geo_capnp.UTMCoord)
    assert round(coord.h) == 5762927


def test_an_untagged_ip_falls_back_to_the_configured_type() -> None:
    writer = run([latlon_ip(tagged=False)], from_name="LatLon", to_name="utm32n")
    assert len(standard(writer)) == 1


def test_the_coordinate_can_come_from_an_attribute() -> None:
    ip = fbp_capnp.IP.new_message(content="payload")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "coord"
    kvs[0].value = geo_capnp.LatLonCoord.new_message(lat=52.0, lon=11.0)
    kvs[0].valueType = LATLON_TYPE

    writer = run([PortMessage(PortValue(ip))], from_attr="coord", to_name="utm32n")
    coord = standard(writer)[0].content.as_struct(geo_capnp.UTMCoord)
    assert coord.zone == 32


def test_the_result_can_go_to_an_attribute_keeping_the_content() -> None:
    writer = run([latlon_ip()], to_name="utm32n", to_attr="utm")
    out = standard(writer)[0]
    assert out.content.as_struct(geo_capnp.LatLonCoord).lat == 52.0
    attr = next(kv for kv in out.attributes if kv.key == "utm")
    assert attr._has("valueType")
    assert attr.value.as_struct(geo_capnp.UTMCoord).zone == 32


def test_an_unknown_target_crs_stops_the_component() -> None:
    assert run([latlon_ip()], to_name="nonsense").values == []


def test_an_unreadable_coordinate_is_skipped() -> None:
    writer = run([ip_message("not a coordinate")], from_name="LatLon", to_name="utm32n")
    assert standard(writer) == []


def test_attributes_are_preserved() -> None:
    writer = run([latlon_ip(site="s1")], to_name="utm32n")
    assert [kv.key for kv in standard(writer)[0].attributes] == ["site"]


def test_bracket_ips_pass_through() -> None:
    writer = run([open_bracket_message(), latlon_ip(), close_bracket_message()], to_name="utm32n")
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_to_geo_coord_output_feeds_straight_into_this() -> None:
    """The two geo components compose now that to_geo_coord tags what it emits."""
    from zalfmas_fbp.components.geo.to_geo_coord import METADATA as TO_COORD_META
    from zalfmas_fbp.components.geo.to_geo_coord import ToGeoCoord

    made = run_process_component(
        ToGeoCoord(TO_COORD_META),
        inputs={"vals": [ip_message(json.dumps([11.0, 52.0])), done_message()]},
        outputs=("coord",),
    ).output("coord")
    assert made.values[0].sysAttributes.contentType == LATLON_TYPE

    writer = run([PortMessage(PortValue(v)) for v in made.values], to_name="utm32n")
    assert standard(writer)[0].content.as_struct(geo_capnp.UTMCoord).zone == 32
