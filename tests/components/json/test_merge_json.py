from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.json.merge_json import METADATA, Config, MergeJson, merge


def run(bases, patches, **settings):
    ports: dict = {
        "in": [*bases, done_message()],
        "patch": [*patches, done_message()],
    }
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(MergeJson(METADATA), inputs=ports, outputs=("out",)).output()


def merged(writer):
    return [json.loads(v.content.as_text()) for v in writer.values if str(v.type) == "standard"]


# --- the merge function ------------------------------------------------------------------------


def test_deep_merge_combines_nested_objects() -> None:
    base = {"a": {"x": 1, "y": 2}, "b": 1}
    patch = {"a": {"y": 20, "z": 30}}
    assert merge(base, patch, Config()) == {"a": {"x": 1, "y": 20, "z": 30}, "b": 1}


def test_shallow_merge_replaces_whole_values() -> None:
    base = {"a": {"x": 1}}
    patch = {"a": {"y": 2}}
    assert merge(base, patch, Config(strategy="shallow")) == {"a": {"y": 2}}


def test_replace_takes_the_patch_entirely() -> None:
    assert merge({"a": 1}, {"b": 2}, Config(strategy="replace")) == {"b": 2}


def test_the_base_can_win_conflicts() -> None:
    assert merge({"a": 1}, {"a": 2}, Config(patch_wins=False)) == {"a": 1}


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [("replace", [3, 4]), ("append", [1, 2, 3, 4])],
)
def test_list_strategies(strategy, expected) -> None:
    result = merge({"l": [1, 2]}, {"l": [3, 4]}, Config(list_strategy=strategy))
    assert result["l"] == expected


def test_lists_can_be_merged_item_by_item_on_a_key() -> None:
    base = {"rows": [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]}
    patch = {"rows": [{"id": 2, "v": "B"}, {"id": 3, "v": "c"}]}
    result = merge(base, patch, Config(list_strategy="by_key"))
    assert result["rows"] == [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 3, "v": "c"}]


def test_a_null_is_a_value_by_default() -> None:
    assert merge({"a": 1}, {"a": None}, Config()) == {"a": None}


def test_a_null_can_be_made_to_delete() -> None:
    assert merge({"a": 1, "b": 2}, {"a": None}, Config(null_deletes=True)) == {"b": 2}


def test_keys_only_in_the_patch_are_added() -> None:
    assert merge({"a": 1}, {"b": 2}, Config()) == {"a": 1, "b": 2}


# --- the component -----------------------------------------------------------------------------


def test_merges_each_base_with_its_patch() -> None:
    writer = run(
        [ip_message(json.dumps({"a": 1})), ip_message(json.dumps({"a": 2}))],
        [ip_message(json.dumps({"b": 10})), ip_message(json.dumps({"b": 20}))],
    )
    assert merged(writer) == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]


def test_attributes_of_both_sides_are_carried_over() -> None:
    from mas.schema.fbp import fbp_capnp

    from tests.component_harness import PortMessage, PortValue
    from zalfmas_fbp.components.common.values import VALUE_TYPE

    def with_attr(content, **attrs):
        ip = fbp_capnp.IP.new_message(content=content)
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            kvs[i].value = common_capnp.Value.new_message(t=value)
            kvs[i].valueType = VALUE_TYPE
        return PortMessage(PortValue(ip))

    writer = run([with_attr(json.dumps({}), base="b")], [with_attr(json.dumps({}), patch="p")])
    assert {kv.key for kv in writer.values[0].attributes} == {"base", "patch"}


def test_the_output_is_tagged_as_json() -> None:
    writer = run([ip_message("{}")], [ip_message("{}")])
    assert writer.values[0].sysAttributes.contentType == "Text (JSON)"


def test_a_malformed_document_forwards_the_base_unchanged() -> None:
    writer = run([ip_message(json.dumps({"a": 1}))], [ip_message("{not json")])
    assert merged(writer) == [{"a": 1}]


def test_the_base_is_forwarded_when_the_patch_stream_ends_early() -> None:
    writer = run([ip_message(json.dumps({"a": 1})), ip_message(json.dumps({"a": 2}))], [])
    assert merged(writer) == [{"a": 1}]


def test_bracket_ips_pass_through_without_consuming_a_patch() -> None:
    writer = run(
        [open_bracket_message(), ip_message(json.dumps({"a": 1})), close_bracket_message()],
        [ip_message(json.dumps({"b": 2}))],
    )
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]
    assert merged(writer) == [{"a": 1, "b": 2}]
