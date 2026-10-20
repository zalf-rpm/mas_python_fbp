from __future__ import annotations

import json

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import STRUCTURED_TEXT_TYPE, VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.ip.merge_ips import METADATA, MergeIPs


def typed_ip(content, content_type=None, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    if content_type:
        ip.sysAttributes.contentType = content_type
    if attrs:
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            kvs[i].value = common_capnp.Value.new_message(t=value)
            kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(slots, **settings):
    """slots: one message list per array in-port slot."""
    ports: dict = {}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(
        MergeIPs(METADATA),
        inputs=ports,
        outputs=("out",),
        array_inputs={"in": [[*msgs, done_message()] for msgs in slots]},
    ).output()


def texts(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def shapes(writer):
    return [str(v.type) for v in writer.values]


# --- next_available --------------------------------------------------------------------------


def test_interleaves_every_input_into_one_stream() -> None:
    writer = run([[ip_message("a1"), ip_message("a2")], [ip_message("b1")]])
    assert sorted(texts(writer)) == ["a1", "a2", "b1"]


def test_it_drains_all_inputs_before_finishing() -> None:
    writer = run([[ip_message(f"a{i}") for i in range(3)], [ip_message(f"b{i}") for i in range(3)]])
    assert len(texts(writer)) == 6


def test_the_source_input_can_be_recorded_as_an_attribute() -> None:
    """The inverse of routing: after a merge, provenance is otherwise lost."""
    writer = run([[ip_message("a")], [ip_message("b")]], tag_source_attr="source")
    by_text = {v.content.as_text(): v for v in writer.values}
    assert python_from_attr(by_text["a"].attributes[0]) == 0
    assert python_from_attr(by_text["b"].attributes[0]) == 1


def test_existing_attributes_survive_tagging() -> None:
    writer = run([[typed_ip("a", None, region="north")]], tag_source_attr="source")
    assert {kv.key for kv in writer.values[0].attributes} == {"region", "source"}


def test_an_unconnected_input_array_merges_nothing() -> None:
    writer = run([])
    assert writer.values == []


# --- zip -------------------------------------------------------------------------------------


def test_zip_groups_one_ip_per_input_into_a_substream() -> None:
    writer = run([[ip_message("a1"), ip_message("a2")], [ip_message("b1"), ip_message("b2")]], strategy="zip")
    assert shapes(writer) == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert texts(writer) == ["a1", "b1", "a2", "b2"]


def test_zip_stops_when_the_shortest_input_closes() -> None:
    writer = run([[ip_message("a1"), ip_message("a2")], [ip_message("b1")]], strategy="zip")
    assert texts(writer) == ["a1", "b1"]


def test_zip_can_fold_the_others_into_attributes_of_the_first() -> None:
    writer = run(
        [[typed_ip("primary")], [typed_ip("secondary")]],
        strategy="zip",
        zip_mode="attributes",
    )
    assert texts(writer) == ["primary"]
    assert [kv.key for kv in writer.values[0].attributes] == ["in1"]
    assert writer.values[0].attributes[0].value.as_text() == "secondary"


def test_the_attribute_prefix_is_configurable() -> None:
    writer = run(
        [[typed_ip("primary")], [typed_ip("secondary")]],
        strategy="zip",
        zip_mode="attributes",
        zip_attr_prefix="src",
    )
    assert [kv.key for kv in writer.values[0].attributes] == ["src1"]


def test_zip_can_build_a_json_object_keyed_by_input_index() -> None:
    writer = run(
        [[ip_message("first")], [ip_message("second")]],
        strategy="zip",
        zip_mode="json_object",
    )
    assert json.loads(writer.values[0].content.as_text()) == {"0": "first", "1": "second"}
    assert writer.values[0].sysAttributes.contentType == "Text (JSON)"


def test_a_json_object_group_is_skipped_when_a_content_type_cannot_be_resolved() -> None:
    """D14: guessing the schema would silently misread it."""
    untyped = PortMessage(PortValue(fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(i64=1))))
    writer = run([[ip_message("ok")], [untyped]], strategy="zip", zip_mode="json_object")
    assert writer.values == []


def test_typed_content_is_converted_for_a_json_object_group() -> None:
    """StructuredText is unwrapped and parsed rather than emitted as its {type, value} struct."""
    typed = typed_ip(
        common_capnp.StructuredText.new_message(type="json", value='{"a": 1}'),
        STRUCTURED_TEXT_TYPE,
    )
    writer = run([[ip_message("ok")], [typed]], strategy="zip", zip_mode="json_object")
    assert json.loads(writer.values[0].content.as_text()) == {"0": "ok", "1": {"a": 1}}
