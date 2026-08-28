from __future__ import annotations

from mas.schema.common import common_capnp
from zalfmas_common import common

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
    text_outputs,
)
from zalfmas_fbp.components.ip.split_bracketed_stream import METADATA, Component


def _substream_length(ip) -> int:
    value = common.get_fbp_attr(ip, "substream_length", common_capnp.Value)
    assert value is not None
    return value.ui64


def test_splits_single_substream_and_counts_its_ips() -> None:
    component = Component(METADATA)

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
        outputs=("out", "brackets"),
    )

    assert text_outputs(result.output("out")) == ["a", "b"]

    bracket_ips = result.output("brackets").values
    assert [ip.type for ip in bracket_ips] == ["openBracket", "closeBracket"]
    assert _substream_length(bracket_ips[1]) == 2


def test_resets_counter_between_consecutive_substreams() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message("x"),
                close_bracket_message(),
                open_bracket_message(),
                ip_message("y"),
                ip_message("z"),
                ip_message("w"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out", "brackets"),
    )

    assert text_outputs(result.output("out")) == ["x", "y", "z", "w"]

    bracket_ips = result.output("brackets").values
    close_ips = [ip for ip in bracket_ips if ip.type == "closeBracket"]
    assert len(close_ips) == 2
    assert _substream_length(close_ips[0]) == 1
    assert _substream_length(close_ips[1]) == 3


def test_forwards_unbracketed_ips_without_touching_brackets_port() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("a"),
                ip_message("b"),
                done_message(),
            ],
        },
        outputs=("out", "brackets"),
    )

    assert text_outputs(result.output("out")) == ["a", "b"]
    assert result.output("brackets").values == []
