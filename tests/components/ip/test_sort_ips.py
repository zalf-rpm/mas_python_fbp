from __future__ import annotations

from typing import Any

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    InMemoryWriter,
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.ip.sort_ips import METADATA, SortIPs


def _ip(content: str, attr_name: str | None = None, **value_kwargs: Any) -> PortMessage:
    attributes = []
    if attr_name is not None:
        attributes = [{"key": attr_name, "value": common_capnp.Value.new_message(**value_kwargs)}]
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content, attributes=attributes)))


def _outputs(writer: InMemoryWriter) -> list[str]:
    result = []
    for ip in writer.values:
        result.append(ip.content.as_text() if ip.type == "standard" else ip.type)
    return result


def _component(**config: Any) -> SortIPs:
    component = SortIPs(METADATA)
    if config:
        component.apply_config_values(config)
    return component


def test_sort_ips_flat_mode_passes_ips_through_and_drops_brackets() -> None:
    component = _component(sort_substreams=False)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message("a"),
                ip_message("b"),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["a", "b"]


def test_sort_ips_sorts_leaf_substream_by_numeric_attr_ascending() -> None:
    component = _component(sort_attr="prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("c", "prio", i64=3),
                _ip("a", "prio", i64=1),
                _ip("b", "prio", i64=2),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "a", "b", "c", "closeBracket"]


def test_sort_ips_sorts_leaf_substream_by_string_attr_lexically() -> None:
    component = _component(sort_attr="name")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("1", "name", t="banana"),
                _ip("2", "name", t="apple"),
                _ip("3", "name", t="cherry"),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "2", "1", "3", "closeBracket"]


def test_sort_ips_supports_at_prefixed_sort_attr_name() -> None:
    component = _component(sort_attr="@prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("b", "prio", i64=2),
                _ip("a", "prio", i64=1),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "a", "b", "closeBracket"]


def test_sort_ips_nested_substream_only_sorts_the_leaf_level() -> None:
    component = _component(sort_attr="prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),  # outer, non-leaf: contains only the inner substream
                open_bracket_message(),  # inner, leaf: gets sorted
                _ip("c", "prio", i64=3),
                _ip("a", "prio", i64=1),
                _ip("b", "prio", i64=2),
                close_bracket_message(),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == [
        "openBracket",
        "openBracket",
        "a",
        "b",
        "c",
        "closeBracket",
        "closeBracket",
    ]


def test_sort_ips_non_leaf_level_keeps_plain_ips_in_original_positions() -> None:
    component = _component(sort_attr="prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),  # outer, non-leaf: mixes a plain IP with a nested substream
                _ip("x", "prio", i64=99),
                open_bracket_message(),
                _ip("c", "prio", i64=3),
                _ip("a", "prio", i64=1),
                close_bracket_message(),
                _ip("y", "prio", i64=-1),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == [
        "openBracket",
        "x",
        "openBracket",
        "a",
        "c",
        "closeBracket",
        "y",
        "closeBracket",
    ]


def test_sort_ips_missing_sort_attr_sorts_that_ip_last() -> None:
    component = _component(sort_attr="prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("no-attr"),
                _ip("b", "prio", i64=2),
                _ip("a", "prio", i64=1),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "a", "b", "no-attr", "closeBracket"]


def test_sort_ips_empty_sort_attr_preserves_received_order() -> None:
    component = _component()  # sort_attr defaults to ""

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("c", "prio", i64=3),
                _ip("a", "prio", i64=1),
                _ip("b", "prio", i64=2),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "c", "a", "b", "closeBracket"]


def test_sort_ips_mixed_value_types_falls_back_to_received_order() -> None:
    component = _component(sort_attr="prio")

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                _ip("c", "prio", i64=3),
                _ip("a", "prio", t="not-a-number"),
                close_bracket_message(),
                done_message(),
            ],
        },
    )

    assert _outputs(result.output()) == ["openBracket", "c", "a", "closeBracket"]
