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
from zalfmas_fbp.components.ip.route_ips import METADATA, RouteIPs


def attr_ip(content, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, slots=2, outputs=("default",), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(
        RouteIPs(METADATA),
        inputs=ports,
        outputs=outputs,
        array_outputs={"out": slots},
    )


def slot_texts(result, index):
    return [v.content.as_text() for v in result.array_output("out")[index].values if str(v.type) == "standard"]


def slot_shapes(result, index):
    return [str(v.type) for v in result.array_output("out")[index].values]


NORTH_SOUTH = [
    {"left": "@region", "op": "eq", "right": "north"},
    {"left": "@region", "op": "eq", "right": "south"},
]


def test_routes_by_content_to_the_matching_slot() -> None:
    result = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south"), attr_ip("c", region="north")],
        routes=NORTH_SOUTH,
    )
    assert slot_texts(result, 0) == ["a", "c"]
    assert slot_texts(result, 1) == ["b"]


def test_unmatched_ips_go_to_default() -> None:
    result = run([attr_ip("x", region="east")], routes=NORTH_SOUTH)
    assert slot_texts(result, 0) == []
    assert [v.content.as_text() for v in result.output("default").values] == ["x"]


def test_unmatched_ips_are_dropped_when_default_is_unconnected() -> None:
    result = run([attr_ip("x", region="east")], outputs=(), routes=NORTH_SOUTH)
    assert slot_texts(result, 0) == []


def test_first_match_only_by_default() -> None:
    both = [{"left": "@region", "op": "eq", "right": "north"}, {"left": "#type", "op": "eq", "right": "standard"}]
    result = run([attr_ip("a", region="north")], routes=both)
    assert slot_texts(result, 0) == ["a"]
    assert slot_texts(result, 1) == []


def test_broadcasting_to_every_match_can_be_turned_on() -> None:
    both = [{"left": "@region", "op": "eq", "right": "north"}, {"left": "#type", "op": "eq", "right": "standard"}]
    result = run([attr_ip("a", region="north")], routes=both, first_match_only=False)
    assert slot_texts(result, 0) == ["a"]
    assert slot_texts(result, 1) == ["a"]


def test_brackets_are_broadcast_so_every_branch_stays_well_formed() -> None:
    result = run(
        [open_bracket_message(), attr_ip("a", region="north"), close_bracket_message()],
        routes=NORTH_SOUTH,
    )
    assert slot_shapes(result, 0) == ["openBracket", "standard", "closeBracket"]
    assert slot_shapes(result, 1) == ["openBracket", "closeBracket"]


def test_routes_beyond_the_connected_slots_are_ignored_not_an_error() -> None:
    three = [*NORTH_SOUTH, {"left": "@region", "op": "eq", "right": "east"}]
    result = run([attr_ip("x", region="east")], slots=2, routes=three)
    assert [v.content.as_text() for v in result.output("default").values] == ["x"]


def test_without_routes_everything_defaults() -> None:
    result = run([ip_message("a"), ip_message("b")], routes=[])
    assert [v.content.as_text() for v in result.output("default").values] == ["a", "b"]


def test_routing_on_a_json_content_path() -> None:
    result = run(
        [ip_message(json.dumps({"yield": 9})), ip_message(json.dumps({"yield": 1}))],
        routes=[{"left": "./yield", "op": "gt", "right": 5}, {"left": "./yield", "op": "le", "right": 5}],
    )
    assert [json.loads(t)["yield"] for t in slot_texts(result, 0)] == [9]
    assert [json.loads(t)["yield"] for t in slot_texts(result, 1)] == [1]
