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
from zalfmas_fbp.components.ip.filter_ips import METADATA, FilterIPs


def attr_ip(content, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = (
            common_capnp.Value.new_message(t=value)
            if isinstance(value, str)
            else common_capnp.Value.new_message(i64=value)
        )
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, outputs=("out", "rej"), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(FilterIPs(METADATA), inputs=ports, outputs=outputs)


def texts(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def shapes(writer):
    return [str(v.type) for v in writer.values]


# --- predicates on attributes and content -----------------------------------------------------


def test_filters_on_an_attribute() -> None:
    result = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south")],
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert texts(result.output("out")) == ["a"]
    assert texts(result.output("rej")) == ["b"]


def test_filters_on_a_json_content_path() -> None:
    result = run(
        [ip_message(json.dumps({"yield": 7.1})), ip_message(json.dumps({"yield": 2.0}))],
        predicate={"left": "./yield", "op": "gt", "right": 5.0},
    )
    assert [json.loads(t)["yield"] for t in texts(result.output("out"))] == [7.1]


def test_filters_on_ip_metadata() -> None:
    result = run(
        [ip_message("a")],
        predicate={"left": "#type", "op": "eq", "right": "standard"},
    )
    assert texts(result.output("out")) == ["a"]


def test_combinators_nest() -> None:
    result = run(
        [attr_ip("a", region="north", year=2020), attr_ip("b", region="north", year=1990)],
        predicate={
            "all": [
                {"left": "@region", "op": "eq", "right": "north"},
                {"left": "@year", "op": "ge", "right": 2000},
            ],
        },
    )
    assert texts(result.output("out")) == ["a"]
    assert texts(result.output("rej")) == ["b"]


def test_invert_swaps_the_outputs() -> None:
    result = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south")],
        predicate={"left": "@region", "op": "eq", "right": "north"},
        invert=True,
    )
    assert texts(result.output("out")) == ["b"]
    assert texts(result.output("rej")) == ["a"]


def test_an_unresolvable_selector_rejects_rather_than_failing() -> None:
    result = run([ip_message("a")], predicate={"left": "@missing", "op": "eq", "right": "x"})
    assert texts(result.output("out")) == []
    assert texts(result.output("rej")) == ["a"]


def test_rejected_ips_are_dropped_when_rej_is_unconnected() -> None:
    result = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south")],
        outputs=("out",),
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert texts(result.output("out")) == ["a"]


# --- substreams ------------------------------------------------------------------------------


def test_substreams_are_preserved_on_both_outputs() -> None:
    result = run(
        [
            open_bracket_message(),
            attr_ip("a", region="north"),
            attr_ip("b", region="south"),
            close_bracket_message(),
        ],
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert shapes(result.output("out")) == ["openBracket", "standard", "closeBracket"]
    assert shapes(result.output("rej")) == ["openBracket", "standard", "closeBracket"]


def test_a_substream_with_nothing_accepted_emits_no_brackets_on_out() -> None:
    result = run(
        [open_bracket_message(), attr_ip("b", region="south"), close_bracket_message()],
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert shapes(result.output("out")) == []
    assert shapes(result.output("rej")) == ["openBracket", "standard", "closeBracket"]


def test_empty_substreams_can_be_kept() -> None:
    result = run(
        [open_bracket_message(), attr_ip("b", region="south"), close_bracket_message()],
        predicate={"left": "@region", "op": "eq", "right": "north"},
        drop_empty_substreams=False,
    )
    assert shapes(result.output("out")) == ["openBracket", "closeBracket"]


def test_nested_substreams_keep_their_nesting() -> None:
    result = run(
        [
            open_bracket_message(),
            open_bracket_message(),
            attr_ip("a", region="north"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        outputs=("out",),
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert shapes(result.output("out")) == [
        "openBracket",
        "openBracket",
        "standard",
        "closeBracket",
        "closeBracket",
    ]


def test_an_empty_inner_substream_is_dropped_but_its_parent_survives() -> None:
    result = run(
        [
            open_bracket_message(),
            attr_ip("a", region="north"),
            open_bracket_message(),
            attr_ip("b", region="south"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        outputs=("out",),
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert shapes(result.output("out")) == ["openBracket", "standard", "closeBracket"]


def test_an_unbalanced_close_bracket_is_forwarded() -> None:
    result = run(
        [attr_ip("a", region="north"), close_bracket_message()],
        outputs=("out",),
        predicate={"left": "@region", "op": "eq", "right": "north"},
    )
    assert shapes(result.output("out")) == ["standard", "closeBracket"]
