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
from zalfmas_fbp.components.ip.group_into_substreams import METADATA, GroupIntoSubstreams


def attr_ip(content, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
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
    return run_process_component(GroupIntoSubstreams(METADATA), inputs=ports, outputs=("out",)).output()


def grouping(writer):
    """The emitted stream as nested lists of contents, one list per substream."""
    groups: list[list[str]] = []
    for value in writer.values:
        kind = str(value.type)
        if kind == "openBracket":
            groups.append([])
        elif kind == "standard":
            groups[-1].append(value.content.as_text())
    return groups


def keys(writer):
    return [python_from_attr(v.attributes[0]) for v in writer.values if str(v.type) == "openBracket"]


def test_groups_consecutive_ips_sharing_a_key() -> None:
    writer = run(
        [
            attr_ip("a", region="north"),
            attr_ip("b", region="north"),
            attr_ip("c", region="south"),
        ],
        selector="@region",
    )
    assert grouping(writer) == [["a", "b"], ["c"]]


def test_the_key_is_attached_to_the_open_bracket() -> None:
    writer = run([attr_ip("a", region="north"), attr_ip("b", region="south")], selector="@region")
    assert keys(writer) == ["north", "south"]


def test_a_key_returning_after_a_gap_opens_a_new_group_in_key_change_mode() -> None:
    """on_key_change holds one group in memory, so it needs the stream ordered by key."""
    writer = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south"), attr_ip("c", region="north")],
        selector="@region",
    )
    assert grouping(writer) == [["a"], ["b"], ["c"]]


def test_buffer_all_collects_a_key_wherever_it_appears() -> None:
    writer = run(
        [attr_ip("a", region="north"), attr_ip("b", region="south"), attr_ip("c", region="north")],
        selector="@region",
        mode="buffer_all",
    )
    assert grouping(writer) == [["a", "c"], ["b"]]
    assert keys(writer) == ["north", "south"]


def test_groups_on_a_json_content_path() -> None:
    writer = run(
        [
            ip_message(json.dumps({"site": "s1", "v": 1})),
            ip_message(json.dumps({"site": "s1", "v": 2})),
            ip_message(json.dumps({"site": "s2", "v": 3})),
        ],
        selector="./site",
    )
    assert [len(group) for group in grouping(writer)] == [2, 1]


def test_max_group_size_splits_oversized_groups() -> None:
    writer = run(
        [attr_ip(str(i), region="north") for i in range(5)],
        selector="@region",
        max_group_size=2,
    )
    assert grouping(writer) == [["0", "1"], ["2", "3"], ["4"]]


def test_max_group_size_applies_to_buffer_all_too() -> None:
    writer = run(
        [attr_ip(str(i), region="north") for i in range(3)],
        selector="@region",
        mode="buffer_all",
        max_group_size=2,
    )
    assert grouping(writer) == [["0", "1"], ["2"]]


def test_an_unresolvable_key_groups_under_none_rather_than_failing() -> None:
    writer = run([ip_message("a"), ip_message("b")], selector="@missing")
    assert grouping(writer) == [["a", "b"]]


def test_the_key_attribute_can_be_suppressed_with_null() -> None:
    """A null config value is the null value, not a request to restore the default."""
    writer = run([attr_ip("a", region="north")], selector="@region", key_attr=None)
    opens = [v for v in writer.values if str(v.type) == "openBracket"]
    assert list(opens[0].attributes) == []


def test_an_empty_name_also_suppresses_it() -> None:
    writer = run([attr_ip("a", region="north")], selector="@region", key_attr="")
    opens = [v for v in writer.values if str(v.type) == "openBracket"]
    assert list(opens[0].attributes) == []


def test_incoming_brackets_are_replaced_by_the_new_grouping() -> None:
    """Keeping both would nest unpredictably, so the incoming grouping is dropped."""
    writer = run(
        [open_bracket_message(), attr_ip("a", region="north"), close_bracket_message()],
        selector="@region",
    )
    assert grouping(writer) == [["a"]]
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_an_empty_stream_emits_nothing() -> None:
    assert run([], selector="@region").values == []
