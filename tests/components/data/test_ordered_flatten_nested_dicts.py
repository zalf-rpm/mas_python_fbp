"""ordered_flatten_nested_dicts, converted from Runnable to Process style (plan LP1)."""

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
from zalfmas_fbp.components.common.values import VALUE_TYPE
from zalfmas_fbp.components.data.transform.ordered_flatten_nested_dicts import (
    METADATA,
    OrderedFlattenNestedDicts,
    ordered_flatten,
)

NESTED = {"b": {"y": 2, "x": 1}, "a": 0}


def run(messages, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(OrderedFlattenNestedDicts(METADATA), inputs=inputs, outputs=("out",)).output()


def results(writer):
    return [json.loads(v.content.as_text()) for v in writer.values if str(v.type) == "standard"]


# --- the flatten function ------------------------------------------------------------------------


def test_leaves_come_out_in_sorted_key_order() -> None:
    assert ordered_flatten(NESTED, False) == [0, 1, 2]


def test_reverse_sorts_every_level_descending() -> None:
    assert ordered_flatten(NESTED, True) == [2, 1, 0]


def test_a_list_of_flags_applies_level_by_level() -> None:
    """Outer level descending, inner ascending."""
    assert ordered_flatten(NESTED, [True, False]) == [1, 2, 0]


def test_levels_beyond_the_list_sort_ascending() -> None:
    assert ordered_flatten({"b": {"y": 2, "x": 1}, "a": 0}, [True]) == [1, 2, 0]


def test_an_empty_flag_list_sorts_ascending() -> None:
    assert ordered_flatten(NESTED, []) == [0, 1, 2]


def test_deeper_nesting_is_followed() -> None:
    assert ordered_flatten({"a": {"b": {"c": 1, "d": 2}}}, False) == [1, 2]


def test_a_non_object_is_a_leaf_in_itself() -> None:
    assert ordered_flatten(42, False) == [42]


def test_lists_are_leaves_not_traversed() -> None:
    assert ordered_flatten({"a": [1, 2]}, False) == [[1, 2]]


# --- the component -------------------------------------------------------------------------------


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_flattens_without_needing_any_config() -> None:
    """The Runnable version read config['reverse'] with no default, so an unconfigured instance
    raised KeyError for every IP - swallowed by a bare except, silently dropping the stream.
    """
    assert results(run([ip_message(json.dumps(NESTED))])) == [[0, 1, 2]]


def test_reverse_can_be_configured() -> None:
    assert results(run([ip_message(json.dumps(NESTED))], reverse=True)) == [[2, 1, 0]]


def test_per_level_flags_can_be_configured() -> None:
    assert results(run([ip_message(json.dumps(NESTED))], reverse=[True, False])) == [[1, 2, 0]]


def test_a_traversal_path_selects_what_to_flatten() -> None:
    payload = {"meta": "ignored", "data": {"b": 2, "a": 1}}
    assert results(run([ip_message(json.dumps(payload))], traversal_path="data")) == [[1, 2]]


def test_an_unresolvable_traversal_path_skips_the_ip() -> None:
    assert results(run([ip_message(json.dumps(NESTED))], traversal_path="nope")) == []


def test_malformed_json_is_skipped() -> None:
    assert results(run([ip_message("{not json"), ip_message(json.dumps(NESTED))])) == [[0, 1, 2]]


def test_malformed_json_can_be_passed_through() -> None:
    writer = run([ip_message("{not json")], on_error="pass_through")
    assert [v.content.as_text() for v in writer.values] == ["{not json"]


def test_the_output_is_tagged_as_json() -> None:
    writer = run([ip_message(json.dumps(NESTED))])
    assert writer.values[0].sysAttributes.contentType == "Text (JSON)"


def test_attributes_are_preserved() -> None:
    ip = fbp_capnp.IP.new_message(content=json.dumps(NESTED))
    kvs = ip.init("attributes", 1)
    kvs[0].key = "run"
    kvs[0].value = common_capnp.Value.new_message(t="r1")
    kvs[0].valueType = VALUE_TYPE

    writer = run([PortMessage(PortValue(ip))])
    assert [kv.key for kv in writer.values[0].attributes] == ["run"]


def test_bracket_ips_pass_through() -> None:
    writer = run([open_bracket_message(), ip_message(json.dumps(NESTED)), close_bracket_message()])
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]
