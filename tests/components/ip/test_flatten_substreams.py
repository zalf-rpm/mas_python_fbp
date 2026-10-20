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
from zalfmas_fbp.components.ip.flatten_substreams import METADATA, FlattenSubstreams


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def open_bracket_with(**attrs):
    ip = fbp_capnp.IP.new_message(type="openBracket")
    entries = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        entries[i].key = key
        entries[i].value = common_capnp.Value.new_message(t=value)
        entries[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(FlattenSubstreams(METADATA), inputs=ports).output()


def shapes(writer):
    return [str(value.type) for value in writer.values]


def contents(writer):
    return [value.content.as_text() for value in writer.values if str(value.type) == "standard"]


def test_strips_the_outermost_bracket_pair_by_default() -> None:
    writer = run([open_bracket_message(), ip_message("a"), ip_message("b"), close_bracket_message()])
    assert shapes(writer) == ["standard", "standard"]
    assert contents(writer) == ["a", "b"]


def test_inner_substreams_survive_when_only_one_level_is_stripped() -> None:
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
    assert shapes(writer) == ["standard", "openBracket", "standard", "closeBracket"]


def test_levels_zero_strips_every_level() -> None:
    writer = run(
        [
            open_bracket_message(),
            open_bracket_message(),
            ip_message("a"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        levels=0,
    )
    assert shapes(writer) == ["standard"]


def test_from_depth_keeps_the_outer_substream_and_flattens_inside_it() -> None:
    writer = run(
        [
            open_bracket_message(),
            open_bracket_message(),
            ip_message("a"),
            close_bracket_message(),
            open_bracket_message(),
            ip_message("b"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        from_depth=1,
    )
    assert shapes(writer) == ["openBracket", "standard", "standard", "closeBracket"]
    assert contents(writer) == ["a", "b"]


def test_a_stream_without_brackets_passes_through_untouched() -> None:
    writer = run([ip_message("a"), ip_message("b")])
    assert contents(writer) == ["a", "b"]


def test_bracket_attributes_are_merged_onto_the_ips_inside() -> None:
    writer = run([open_bracket_with(group="north"), ip_message("a"), close_bracket_message()])
    assert [python_from_attr(kv) for kv in writer.values[0].attributes] == ["north"]


def test_merging_bracket_attributes_can_be_turned_off() -> None:
    writer = run(
        [open_bracket_with(group="north"), ip_message("a"), close_bracket_message()],
        merge_bracket_attrs=False,
    )
    assert list(writer.values[0].attributes) == []


def test_an_ips_own_attributes_win_over_inherited_bracket_attributes() -> None:
    inner = fbp_capnp.IP.new_message(content="a")
    entries = inner.init("attributes", 1)
    entries[0].key = "group"
    entries[0].value = common_capnp.Value.new_message(t="own")
    entries[0].valueType = VALUE_TYPE

    writer = run([open_bracket_with(group="north"), PortMessage(PortValue(inner)), close_bracket_message()])
    assert [python_from_attr(kv) for kv in writer.values[0].attributes] == ["own"]


def test_inner_bracket_attributes_win_over_outer_ones() -> None:
    writer = run(
        [
            open_bracket_with(group="outer"),
            open_bracket_with(group="inner"),
            ip_message("a"),
            close_bracket_message(),
            close_bracket_message(),
        ],
        levels=0,
    )
    assert [python_from_attr(kv) for kv in writer.values[0].attributes] == ["inner"]


def test_attributes_of_a_kept_bracket_are_not_merged() -> None:
    """Only stripped brackets hand their attributes down; a forwarded bracket keeps its own."""
    writer = run([open_bracket_with(group="north"), ip_message("a"), close_bracket_message()], from_depth=5)
    assert shapes(writer) == ["openBracket", "standard", "closeBracket"]
    assert list(writer.values[1].attributes) == []
    assert [python_from_attr(kv) for kv in writer.values[0].attributes] == ["north"]


def test_an_unbalanced_close_bracket_is_forwarded_rather_than_dropped() -> None:
    writer = run([ip_message("a"), close_bracket_message()])
    assert shapes(writer) == ["standard", "closeBracket"]


def test_an_unterminated_substream_still_emits_what_arrived() -> None:
    writer = run([open_bracket_message(), ip_message("a")])
    assert contents(writer) == ["a"]
