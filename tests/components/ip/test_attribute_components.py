from __future__ import annotations

import json

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
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.ip.attributes_to_content import METADATA as A2C_META
from zalfmas_fbp.components.ip.attributes_to_content import AttributesToContent
from zalfmas_fbp.components.ip.content_to_attributes import METADATA as C2A_META
from zalfmas_fbp.components.ip.content_to_attributes import ContentToAttributes
from zalfmas_fbp.components.ip.map_attributes import METADATA as MAP_META
from zalfmas_fbp.components.ip.map_attributes import MapAttributes


def attr_ip(content="", **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def run(component, messages, **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(component, inputs=ports, outputs=("out",)).output()


def attrs_of(value):
    return {kv.key: python_from_attr(kv) for kv in value.attributes}


# --- content_to_attributes --------------------------------------------------------------------


def test_lifts_content_paths_into_attributes() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [ip_message(json.dumps({"site": {"id": 42}, "year": 2020}))],
        paths={"site": "./site/id", "year": "./year"},
    )
    assert attrs_of(writer.values[0]) == {"site": 42, "year": 2020}


def test_the_content_is_kept_by_default() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [ip_message(json.dumps({"a": 1}))],
        paths={"a": "./a"},
    )
    assert json.loads(writer.values[0].content.as_text()) == {"a": 1}


def test_the_content_can_be_dropped() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [ip_message(json.dumps({"a": 1}))],
        paths={"a": "./a"},
        keep_content=False,
    )
    assert writer.values[0].content.as_text() == ""


def test_existing_attributes_are_preserved() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [attr_ip(json.dumps({"a": 1}), region="north")],
        paths={"a": "./a"},
    )
    assert attrs_of(writer.values[0]) == {"region": "north", "a": 1}


def test_a_missing_path_is_skipped_by_default() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [ip_message(json.dumps({"a": 1}))],
        paths={"a": "./a", "b": "./nope"},
    )
    assert set(attrs_of(writer.values[0])) == {"a"}


def test_a_missing_path_can_drop_the_ip() -> None:
    writer = run(
        ContentToAttributes(C2A_META),
        [ip_message(json.dumps({"a": 1}))],
        paths={"b": "./nope"},
        on_missing="fail",
    )
    assert writer.values == []


# --- attributes_to_content --------------------------------------------------------------------


def test_gathers_attributes_into_a_json_object() -> None:
    writer = run(AttributesToContent(A2C_META), [attr_ip("x", region="north", year="2020")])
    assert json.loads(writer.values[0].content.as_text()) == {"region": "north", "year": "2020"}


def test_attributes_are_dropped_from_the_output_by_default() -> None:
    writer = run(AttributesToContent(A2C_META), [attr_ip("x", region="north")])
    assert list(writer.values[0].attributes) == []


def test_attributes_can_be_kept_as_well() -> None:
    writer = run(AttributesToContent(A2C_META), [attr_ip("x", region="north")], keep_attributes=True)
    assert attrs_of(writer.values[0]) == {"region": "north"}


def test_a_subset_can_be_selected() -> None:
    writer = run(
        AttributesToContent(A2C_META),
        [attr_ip("x", a="1", b="2", c="3")],
        only=["a", "c"],
    )
    assert set(json.loads(writer.values[0].content.as_text())) == {"a", "c"}


def test_attributes_can_be_excluded() -> None:
    writer = run(AttributesToContent(A2C_META), [attr_ip("x", a="1", b="2")], exclude=["b"])
    assert set(json.loads(writer.values[0].content.as_text())) == {"a"}


def test_the_content_can_be_included_under_a_key() -> None:
    writer = run(
        AttributesToContent(A2C_META),
        [attr_ip("payload", region="north")],
        include_content_under="body",
    )
    assert json.loads(writer.values[0].content.as_text()) == {"region": "north", "body": "payload"}


def test_the_reverse_direction_explodes_an_object_into_attributes() -> None:
    writer = run(
        AttributesToContent(A2C_META),
        [ip_message(json.dumps({"region": "north", "year": 2020}))],
        direction="to_attributes",
    )
    assert attrs_of(writer.values[0]) == {"region": "north", "year": 2020}


def test_the_reverse_direction_forwards_non_objects_unchanged() -> None:
    writer = run(AttributesToContent(A2C_META), [ip_message(json.dumps([1, 2]))], direction="to_attributes")
    assert json.loads(writer.values[0].content.as_text()) == [1, 2]


def test_attributes_and_content_round_trip() -> None:
    gathered = run(AttributesToContent(A2C_META), [attr_ip("x", region="north", year="2020")])
    back = run(
        AttributesToContent(A2C_META),
        [PortMessage(PortValue(v)) for v in gathered.values],
        direction="to_attributes",
    )
    assert attrs_of(back.values[0]) == {"region": "north", "year": "2020"}


# --- map_attributes ---------------------------------------------------------------------------


def test_renames_attributes() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", old="v")], rename={"old": "new"})
    assert attrs_of(writer.values[0]) == {"new": "v"}


def test_keeps_only_the_listed_attributes() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", a="1", b="2")], keep=["a"])
    assert set(attrs_of(writer.values[0])) == {"a"}


def test_drops_the_listed_attributes() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", a="1", b="2")], drop=["b"])
    assert set(attrs_of(writer.values[0])) == {"a"}


def test_defaults_only_fill_what_is_absent() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", a="kept")], defaults={"a": "ignored", "b": "added"})
    assert attrs_of(writer.values[0]) == {"a": "kept", "b": "added"}


def test_set_replaces_what_is_there() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", a="old")], set={"a": "new"})
    assert attrs_of(writer.values[0]) == {"a": "new"}


def test_rename_happens_before_keep_and_drop() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("x", old="v")], rename={"old": "new"}, keep=["new"])
    assert attrs_of(writer.values[0]) == {"new": "v"}


def test_the_content_survives_mapping() -> None:
    writer = run(MapAttributes(MAP_META), [attr_ip("payload", a="1")], drop=["a"])
    assert writer.values[0].content.as_text() == "payload"


def test_brackets_pass_through_every_attribute_component() -> None:
    for component, settings in (
        (ContentToAttributes(C2A_META), {"paths": {"a": "./a"}}),
        (AttributesToContent(A2C_META), {}),
        (MapAttributes(MAP_META), {}),
    ):
        writer = run(component, [open_bracket_message(), ip_message("{}"), close_bracket_message()], **settings)
        assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]
