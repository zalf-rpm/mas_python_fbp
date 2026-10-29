from __future__ import annotations

import asyncio

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.components.common.brackets import (
    Attr,
    BracketOutcome,
    BracketPolicy,
    BracketTracker,
    Substream,
)
from zalfmas_fbp.components.common.values import MISSING, VALUE_TYPE, python_from_attr


def ip(content="x", **attrs):
    message = fbp_capnp.IP.new_message(content=content)
    if attrs:
        kvs = message.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            kvs[i].value = value if isinstance(value, str) else common_capnp.Value.new_message(i64=value)
            if not isinstance(value, str):
                kvs[i].valueType = VALUE_TYPE
    return message.as_reader()


def open_bracket():
    return fbp_capnp.IP.new_message(type="openBracket").as_reader()


def close_bracket():
    return fbp_capnp.IP.new_message(type="closeBracket").as_reader()


def reader_from(messages):
    queue = list(messages)

    async def read():
        return queue.pop(0) if queue else None

    return read


# --- predicates ----------------------------------------------------------------------------


def test_bracket_predicates() -> None:
    assert brackets.is_bracket(open_bracket())
    assert brackets.is_open_bracket(open_bracket())
    assert brackets.is_close_bracket(close_bracket())
    assert not brackets.is_bracket(ip())


# --- attributes ----------------------------------------------------------------------------


def test_copy_attrs_preserves_value_type_and_desc() -> None:
    source = fbp_capnp.IP.new_message()
    kvs = source.init("attributes", 1)
    kvs[0].key = "num"
    kvs[0].value = common_capnp.Value.new_message(i64=42)
    kvs[0].valueType = VALUE_TYPE
    kvs[0].desc = "a number"

    target = fbp_capnp.IP.new_message(content="y")
    brackets.copy_attrs(source.as_reader(), target)

    copied = target.as_reader().attributes[0]
    assert copied.key == "num"
    assert copied._has("valueType")
    assert copied._has("desc")
    assert python_from_attr(copied) == 42


def test_unset_optional_text_fields_stay_unset() -> None:
    """Writing back a read-as-"" Text field would turn "unset" into "explicitly empty" (D4/D14)."""
    source = ip(raw="text")
    target = fbp_capnp.IP.new_message(content="y")
    brackets.copy_attrs(source, target)

    copied = target.as_reader().attributes[0]
    assert not copied._has("valueType")
    assert not copied._has("desc")


def test_plain_python_extras_are_written_as_typed_values() -> None:
    """D4: base components always write common.Value with valueType set."""
    target = fbp_capnp.IP.new_message(content="y")
    brackets.copy_attrs(ip(), target, extra={"count": 7, "label": "abc"})

    written = {kv.key: kv for kv in target.as_reader().attributes}
    assert all(kv._has("valueType") for kv in written.values())
    assert python_from_attr(written["count"]) == 7
    assert python_from_attr(written["label"]) == "abc"


def test_attr_instances_are_written_as_given() -> None:
    target = fbp_capnp.IP.new_message(content="y")
    brackets.set_attrs(target, {"t": Attr(common_capnp.Value.new_message(t="v"), VALUE_TYPE, "described")})

    written = target.as_reader().attributes[0]
    assert (written.valueType, written.desc) == (VALUE_TYPE, "described")


def test_copy_attrs_removes_and_overrides() -> None:
    target = fbp_capnp.IP.new_message(content="y")
    brackets.copy_attrs(ip(a="1", b="2", c="3"), target, extra={"b": 99}, remove={"a"})

    written = {kv.key: python_from_attr(kv) for kv in target.as_reader().attributes}
    assert written == {"b": 99, "c": "3"}


def test_copy_attrs_applies_every_override_not_only_the_first() -> None:
    target = fbp_capnp.IP.new_message(content="y")
    brackets.copy_attrs(ip(a="1", b="2"), target, extra={"a": 10, "b": 20})

    assert {kv.key: python_from_attr(kv) for kv in target.as_reader().attributes} == {"a": 10, "b": 20}


def test_attrs_as_dict_reads_python_values() -> None:
    assert brackets.attrs_as_dict(ip(n=5, s="t")) == {"n": 5, "s": "t"}


def test_attrs_as_dict_reports_missing_for_untyped_structs() -> None:
    message = fbp_capnp.IP.new_message()
    kvs = message.init("attributes", 1)
    kvs[0].key = "untyped"
    kvs[0].value = common_capnp.Value.new_message(i64=1)
    assert brackets.attrs_as_dict(message.as_reader())["untyped"] is MISSING


def test_merge_attrs_lets_later_ips_win() -> None:
    merged = brackets.merge_attrs([ip(a="1", b="2"), ip(b="3")], remove={"a"})
    assert {name: python_from_attr(kv) for name, kv in merged.items()} == {"b": "3"}


def test_make_bracket_builds_typed_brackets_with_attributes() -> None:
    built = brackets.make_bracket("closeBracket", {brackets.SUBSTREAM_LENGTH_ATTR: 3})
    reader = built.as_reader()
    assert str(reader.type) == "closeBracket"
    assert python_from_attr(reader.attributes[0]) == 3


def test_make_bracket_rejects_a_non_bracket_type() -> None:
    with pytest.raises(ValueError, match="not a bracket type"):
        brackets.make_bracket("standard")  # pyright: ignore[reportArgumentType]


# --- policy --------------------------------------------------------------------------------


def test_transparent_policy_forwards_brackets() -> None:
    written = []

    async def write(message):
        written.append(message)
        return True

    assert (
        asyncio.run(brackets.handle_bracket(open_bracket(), BracketPolicy.TRANSPARENT, write)) is BracketOutcome.HANDLED
    )
    assert asyncio.run(brackets.handle_bracket(ip(), BracketPolicy.TRANSPARENT, write)) is BracketOutcome.NOT_A_BRACKET
    assert len(written) == 1


def test_ignore_policy_drops_brackets_without_writing() -> None:
    async def write(_):
        msg = "IGNORE must not write"
        raise AssertionError(msg)

    assert asyncio.run(brackets.handle_bracket(open_bracket(), BracketPolicy.IGNORE, write)) is BracketOutcome.HANDLED


def test_aware_policy_hands_brackets_to_the_component() -> None:
    async def write(_):
        return True

    assert (
        asyncio.run(brackets.handle_bracket(open_bracket(), BracketPolicy.AWARE, write)) is BracketOutcome.NOT_A_BRACKET
    )


def test_a_failed_write_is_reported_rather_than_swallowed() -> None:
    async def write(_):
        return False

    assert (
        asyncio.run(brackets.handle_bracket(open_bracket(), BracketPolicy.TRANSPARENT, write))
        is BracketOutcome.WRITE_FAILED
    )


# --- tracker -------------------------------------------------------------------------------


def test_tracker_counts_nesting() -> None:
    tracker = BracketTracker()
    assert tracker.observe(open_bracket()) == 1
    assert tracker.observe(open_bracket()) == 2
    assert tracker.inside_substream
    assert tracker.observe(ip()) == 2
    assert tracker.observe(close_bracket()) == 1
    assert tracker.observe(close_bracket()) == 0
    assert not tracker.inside_substream


def test_tracker_survives_an_unbalanced_close() -> None:
    tracker = BracketTracker()
    assert tracker.observe(close_bracket()) == 0
    assert tracker.unbalanced_closes == 1


# --- collection ----------------------------------------------------------------------------


def test_collect_a_flat_substream() -> None:
    read = reader_from([ip("a"), ip("b"), close_bracket()])
    substream = asyncio.run(brackets.collect_substream(read, open_bracket()))

    assert [item.content.as_text() for item in substream.ips] == ["a", "b"]
    assert substream.is_leaf
    assert not substream.truncated
    assert substream.close_ip is not None
    assert len(substream) == 2


def test_collect_reads_the_open_bracket_itself_when_not_given_one() -> None:
    read = reader_from([open_bracket(), ip("a"), close_bracket()])
    substream = asyncio.run(brackets.collect_substream(read))
    assert [item.content.as_text() for item in substream.ips] == ["a"]


def test_ips_before_the_open_bracket_are_discarded() -> None:
    read = reader_from([ip("stray"), open_bracket(), ip("a"), close_bracket()])
    substream = asyncio.run(brackets.collect_substream(read))
    assert [item.content.as_text() for item in substream.ips] == ["a"]


def test_nested_substreams_become_a_tree() -> None:
    read = reader_from(
        [ip("a"), open_bracket(), ip("b"), ip("c"), close_bracket(), ip("d"), close_bracket()],
    )
    substream = asyncio.run(brackets.collect_substream(read, open_bracket()))

    assert not substream.is_leaf
    assert [item.content.as_text() for item in substream.ips] == ["a", "d"]
    assert [item.content.as_text() for item in substream.all_ips()] == ["a", "b", "c", "d"]

    nested = [item for item in substream.items if isinstance(item, Substream)]
    assert len(nested) == 1
    assert [item.content.as_text() for item in nested[0].ips] == ["b", "c"]


def test_leaves_finds_the_innermost_substreams() -> None:
    read = reader_from(
        [
            open_bracket(),
            ip("a"),
            close_bracket(),
            open_bracket(),
            ip("b"),
            close_bracket(),
            close_bracket(),
        ],
    )
    substream = asyncio.run(brackets.collect_substream(read, open_bracket()))
    leaves = list(substream.leaves())

    assert len(leaves) == 2
    assert [leaf.ips[0].content.as_text() for leaf in leaves] == ["a", "b"]


def test_a_truncated_substream_keeps_what_was_read() -> None:
    """Losing IPs already taken from the channel is worse than an incomplete group."""
    read = reader_from([ip("a"), ip("b")])
    substream = asyncio.run(brackets.collect_substream(read, open_bracket()))

    assert substream.truncated
    assert substream.close_ip is None
    assert [item.content.as_text() for item in substream.ips] == ["a", "b"]


def test_truncation_propagates_out_of_a_nested_substream() -> None:
    read = reader_from([ip("a"), open_bracket(), ip("b")])
    substream = asyncio.run(brackets.collect_substream(read, open_bracket()))

    assert substream.truncated
    assert [item.content.as_text() for item in substream.all_ips()] == ["a", "b"]


def test_collecting_from_an_exhausted_port_is_truncated_not_an_error() -> None:
    substream = asyncio.run(brackets.collect_substream(reader_from([])))
    assert substream.truncated
    assert substream.open_ip is None
