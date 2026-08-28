from __future__ import annotations

from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.ip.wrap_into_substream import METADATA, WrapIntoSubstream


def test_free_running_mode_unchanged_when_brackets_port_unconnected() -> None:
    # default config (no_of_ips=0) wraps everything received on 'in' into a single
    # substream and closes it once 'in' closes.
    component = WrapIntoSubstream(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("a"),
                ip_message("b"),
                ip_message("c"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "standard",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert [ip.content.as_text() for ip in out[1:4]] == ["a", "b", "c"]


def test_free_running_mode_wraps_fixed_size_batches() -> None:
    component = WrapIntoSubstream(METADATA)
    component.apply_config_values({"no_of_ips": 2})

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("a"),
                ip_message("b"),
                ip_message("c"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
        "openBracket",
        "standard",
        "closeBracket",
    ]
    assert out[1].content.as_text() == "a"
    assert out[2].content.as_text() == "b"
    assert out[5].content.as_text() == "c"


def test_bracket_synced_mode_uses_substream_length_to_bound_substream() -> None:
    component = WrapIntoSubstream(METADATA)

    result = run_process_component(
        component,
        inputs={
            "brackets": [
                open_bracket_message(),
                close_bracket_message(substream_length=common_capnp.Value.new_message(ui64=2)),
                done_message(),
            ],
            "in": [
                ip_message("a"),
                ip_message("b"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == ["openBracket", "standard", "standard", "closeBracket"]
    assert out[1].content.as_text() == "a"
    assert out[2].content.as_text() == "b"
    # substream_length attribute must not leak onto the forwarded close-bracket
    assert list(out[3].attributes) == []


def test_bracket_synced_mode_waits_for_in_ips_even_after_close_bracket_arrives() -> None:
    component = WrapIntoSubstream(METADATA)

    result = run_process_component(
        component,
        inputs={
            "brackets": [
                open_bracket_message(),
                close_bracket_message(substream_length=common_capnp.Value.new_message(ui64=3)),
                done_message(),
            ],
            "in": [
                ip_message("a"),
                ip_message("b"),
                ip_message("c"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "standard",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert [ip.content.as_text() for ip in out[1:4]] == ["a", "b", "c"]


def test_bracket_synced_mode_handles_multiple_substreams() -> None:
    component = WrapIntoSubstream(METADATA)

    result = run_process_component(
        component,
        inputs={
            "brackets": [
                open_bracket_message(),
                close_bracket_message(substream_length=common_capnp.Value.new_message(ui64=1)),
                open_bracket_message(),
                close_bracket_message(substream_length=common_capnp.Value.new_message(ui64=2)),
                done_message(),
            ],
            "in": [
                ip_message("a"),
                ip_message("b"),
                ip_message("c"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "standard",
        "closeBracket",
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert out[1].content.as_text() == "a"
    assert [ip.content.as_text() for ip in out[4:6]] == ["b", "c"]
