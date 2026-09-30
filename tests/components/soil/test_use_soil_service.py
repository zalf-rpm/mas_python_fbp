"""use_soil_service, converted from Runnable to Process style (plan LP3)."""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.soil import soil_capnp

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
from tests.fake_services import FakeSoilProfile, FakeSoilService
from zalfmas_fbp.components.soil.use_soil_service import METADATA, UseSoilService


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


def run(messages, *, service=None, service_messages=None, after=None, **settings):
    made = service if service is not None else FakeSoilService()
    inputs: dict = {
        "latlon": [*messages, done_message()],
        "service": service_messages if service_messages is not None else [cap_message(lambda: made), done_message()],
    }
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    result = run_process_component(UseSoilService(METADATA), inputs=inputs, outputs=("out",), after=after)
    return result, made


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


async def profile_ids(result):
    ids = []
    for value in standard(result.output("out")):
        cap = value.content.as_interface(soil_capnp.Profile)
        ids.append((await cap.info()).id)
    return ids


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_a_profile_at_all() -> None:
    """The Runnable version awaited `service.profilesAt(...).profiles` - a method that does not
    exist (it is `closestProfilesAt`), and an await of the promise's attribute rather than the
    result. The blanket except turned both into silence."""

    result, _ = run([coord_ip()])
    assert len(standard(result.output("out"))) == 1


def test_the_profile_it_sends_is_a_live_capability() -> None:
    service = FakeSoilService(profiles=[FakeSoilProfile(id_="p-7")])
    result, _ = run([coord_ip()], service=service, after=profile_ids)
    assert result.after_result == ["p-7"]


def test_it_asks_the_service_for_the_incoming_coordinate() -> None:
    _, service = run([coord_ip(lat=51.5, lon=12.25)])
    assert service.requested_coords == [(51.5, 12.25)]


def test_outgoing_ips_are_tagged_with_the_profile_type() -> None:
    result, _ = run([coord_ip()])
    assert standard(result.output("out"))[0].sysAttributes.contentType == "soil.capnp:Profile"


def test_the_default_query_asks_for_the_documented_properties() -> None:
    _, service = run([coord_ip()])
    assert service.queries[0]["mandatory"] == ["soilType", "organicCarbon", "rawDensity"]
    assert service.queries[0]["optional"] == []
    assert service.queries[0]["onlyRawData"] is False


def test_the_query_properties_can_be_configured() -> None:
    _, service = run([coord_ip()], mandatory=["sand", "clay"], optional=["pH"], only_raw_data=True)
    assert service.queries[0] == {
        "mandatory": ["sand", "clay"],
        "optional": ["pH"],
        "onlyRawData": True,
    }


def test_an_unknown_soil_property_is_rejected(caplog) -> None:
    """Capnp would raise deep inside the query build; this catches it at config time instead."""

    run([coord_ip()], mandatory=["not_a_property"])
    assert "could not apply config" in caplog.text


def test_only_the_first_profile_is_sent_by_default() -> None:
    service = FakeSoilService(profiles=[FakeSoilProfile(id_="a"), FakeSoilProfile(id_="b")])
    result, _ = run([coord_ip()], service=service, after=profile_ids)
    assert result.after_result == ["a"]


def test_emit_all_sends_every_profile() -> None:
    """A coordinate can be covered by several profiles; the rest used to be dropped silently."""

    service = FakeSoilService(profiles=[FakeSoilProfile(id_="a"), FakeSoilProfile(id_="b")])
    result, _ = run([coord_ip()], service=service, emit="all", after=profile_ids)
    assert result.after_result == ["a", "b"]


def test_emit_all_can_wrap_each_coordinates_profiles_in_a_substream() -> None:
    service = FakeSoilService(profiles=[FakeSoilProfile(id_="a"), FakeSoilProfile(id_="b")])
    result, _ = run([coord_ip()], service=service, emit="all", wrap_in_substream=True)
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
    ]


def test_no_substream_when_only_the_first_is_sent() -> None:
    result, _ = run([coord_ip()], wrap_in_substream=True)
    assert [str(v.type) for v in result.output("out").values] == ["standard"]


def test_a_coordinate_without_profiles_is_skipped_by_default() -> None:
    result, _ = run([coord_ip()], service=FakeSoilService(profiles=[]))
    assert result.output("out").values == []


def test_a_coordinate_without_profiles_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no soil profile"):
        run([coord_ip()], service=FakeSoilService(profiles=[]), on_no_profiles="fail")


def test_from_attr_reads_the_coordinate_out_of_an_attribute() -> None:
    result, service = run([coord_ip(lat=50.0, lon=10.0, to_attr="coord")], from_attr="coord")
    assert service.requested_coords == [(50.0, 10.0)]
    assert len(standard(result.output("out"))) == 1


def test_to_attr_puts_the_profile_in_an_attribute_instead() -> None:
    result, _ = run([coord_ip()], to_attr="soil")
    out = standard(result.output("out"))[0]
    assert "soil" in [entry.key for entry in out.attributes]
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


def test_the_service_is_read_once_for_the_whole_run() -> None:
    result, service = run([coord_ip(), coord_ip(lat=53.0), coord_ip(lat=54.0)])
    assert len(standard(result.output("out"))) == 3
    assert len(service.requested_coords) == 3
