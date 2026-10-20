from __future__ import annotations

import json

import pytest
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
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr, python_from_value
from zalfmas_fbp.components.ip.reduce_substream import METADATA, ReduceSubstream, apply_op


def attr_ip(content="", **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = (
            common_capnp.Value.new_message(t=value)
            if isinstance(value, str)
            else common_capnp.Value.new_message(f64=float(value))
        )
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def open_with(**attrs):
    ip = fbp_capnp.IP.new_message(type="openBracket")
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(ReduceSubstream(METADATA), inputs=ports, outputs=("out",)).output()


def contents(writer):
    return [python_from_value(v.content.as_struct(common_capnp.Value)) for v in writer.values]


def attrs_of(value):
    return {kv.key: python_from_attr(kv) for kv in value.attributes}


# --- the operators ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("op", "values_in", "expected"),
    [
        ("count", [1, 2, 3], 3),
        ("sum", [1, 2, 3], 6.0),
        ("mean", [1, 2, 3], 2.0),
        ("min", [3, 1, 2], 1.0),
        ("max", [3, 1, 2], 3.0),
        ("first", ["a", "b"], "a"),
        ("last", ["a", "b"], "b"),
        ("list", ["a", "b"], ["a", "b"]),
        ("set", ["a", "b", "a"], ["a", "b"]),
        ("concat_text", ["a", "b"], "a, b"),
        ("count", [], 0),
        ("sum", [], None),
    ],
)
def test_operators(op, values_in, expected) -> None:
    assert apply_op(op, values_in) == expected


def test_numeric_operators_ignore_values_that_are_not_numbers() -> None:
    assert apply_op("sum", [1, "two", 3]) == 4.0
    assert apply_op("sum", ["two"]) is None


def test_numeric_text_still_counts_as_a_number() -> None:
    assert apply_op("sum", ["1", "2"]) == 3.0


# --- reducing substreams ------------------------------------------------------------------------


def test_a_substream_becomes_one_ip_carrying_the_count() -> None:
    writer = run([open_bracket_message(), ip_message("a"), ip_message("b"), close_bracket_message()])
    assert contents(writer) == [2]


def test_aggregating_over_an_attribute() -> None:
    writer = run(
        [open_bracket_message(), attr_ip(yield_=3), attr_ip(yield_=5), close_bracket_message()],
        aggregations=[{"selector": "@yield_", "op": "sum", "to_attr": "total", "to_content": True}],
    )
    assert contents(writer) == [8.0]
    assert attrs_of(writer.values[0])["total"] == 8.0


def test_aggregating_over_a_json_content_path() -> None:
    writer = run(
        [
            open_bracket_message(),
            ip_message(json.dumps({"v": 2})),
            ip_message(json.dumps({"v": 4})),
            close_bracket_message(),
        ],
        aggregations=[{"selector": "./v", "op": "mean", "to_attr": "avg", "to_content": True}],
    )
    assert contents(writer) == [3.0]


def test_several_aggregations_become_a_json_object_in_the_content() -> None:
    writer = run(
        [open_bracket_message(), attr_ip(v=1), attr_ip(v=3), close_bracket_message()],
        aggregations=[
            {"selector": "@v", "op": "min", "to_attr": "lo", "to_content": True},
            {"selector": "@v", "op": "max", "to_attr": "hi", "to_content": True},
        ],
    )
    assert json.loads(writer.values[0].content.as_text()) == {"lo": 1.0, "hi": 3.0}


def test_results_can_go_to_attributes_only() -> None:
    writer = run(
        [open_bracket_message(), attr_ip(v=2), close_bracket_message()],
        aggregations=[{"selector": "@v", "op": "sum", "to_attr": "total"}],
    )
    assert attrs_of(writer.values[0])["total"] == 2.0


def test_the_open_brackets_attributes_are_carried_over() -> None:
    """So a group key put there by 'Group IPs into substreams' survives the reduction."""
    writer = run([open_with(group_key="north"), ip_message("a"), close_bracket_message()])
    assert attrs_of(writer.values[0])["group_key"] == "north"


def test_carrying_the_open_bracket_attributes_can_be_turned_off() -> None:
    writer = run(
        [open_with(group_key="north"), ip_message("a"), close_bracket_message()],
        keep_open_bracket_attrs=False,
        aggregations=[{"op": "count", "to_attr": "count"}],
    )
    assert "group_key" not in attrs_of(writer.values[0])


def test_several_substreams_each_produce_one_ip() -> None:
    writer = run(
        [
            open_bracket_message(),
            ip_message("a"),
            close_bracket_message(),
            open_bracket_message(),
            ip_message("b"),
            ip_message("c"),
            close_bracket_message(),
        ],
    )
    assert contents(writer) == [1, 2]


def test_nested_substreams_reduce_at_the_outer_level_by_default() -> None:
    writer = run(
        [
            open_bracket_message(),
            ip_message("a"),
            open_bracket_message(),
            ip_message("b"),
            close_bracket_message(),
            close_bracket_message(),
        ],
    )
    assert contents(writer) == [1]  # only the directly contained IPs


def test_nesting_level_one_reduces_the_inner_substreams_and_keeps_the_outer() -> None:
    writer = run(
        [
            open_bracket_message(),
            open_bracket_message(),
            ip_message("a"),
            ip_message("b"),
            close_bracket_message(),
            open_bracket_message(),
            ip_message("c"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        nesting_level=1,
    )
    shapes = [str(v.type) for v in writer.values]
    assert shapes == ["openBracket", "standard", "standard", "closeBracket"]
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert [python_from_value(v.content.as_struct(common_capnp.Value)) for v in standard] == [2, 1]


def test_pass_through_keeps_the_originals_and_adds_the_reduction() -> None:
    writer = run(
        [open_bracket_message(), ip_message("a"), ip_message("b"), close_bracket_message()],
        pass_through_ips=True,
    )
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "standard", "standard"]


def test_an_ip_outside_a_substream_is_forwarded_unchanged() -> None:
    writer = run([ip_message("loose")])
    assert [v.content.as_text() for v in writer.values] == ["loose"]


def test_a_truncated_substream_is_still_reduced() -> None:
    """Losing IPs already taken from the channel would be worse than an incomplete group."""
    writer = run([open_bracket_message(), ip_message("a"), ip_message("b")])
    assert contents(writer) == [2]


def test_an_empty_substream_reduces_to_zero() -> None:
    writer = run([open_bracket_message(), close_bracket_message()])
    assert contents(writer) == [0]
