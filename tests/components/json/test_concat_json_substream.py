from __future__ import annotations

import json

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    ip_message_with_attrs,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.json.concat_json_substream import METADATA, Component


def test_concatenates_one_substream_into_a_json_list() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps(1)),
                ip_message(json.dumps("a")),
                ip_message(json.dumps({"x": 1})),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert len(out) == 1
    assert json.loads(out[0].content.as_text()) == [1, "a", {"x": 1}]


def test_emits_one_list_per_consecutive_substream() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps(1)),
                close_bracket_message(),
                open_bracket_message(),
                ip_message(json.dumps(2)),
                ip_message(json.dumps(3)),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [json.loads(ip.content.as_text()) for ip in out] == [[1], [2, 3]]


def test_ignores_ip_received_outside_of_a_substream() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message(json.dumps("stray")),
                open_bracket_message(),
                ip_message(json.dumps(1)),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [json.loads(ip.content.as_text()) for ip in out] == [[1]]


def test_skips_invalid_json_inside_substream() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps(1)),
                ip_message("{not valid json"),
                ip_message(json.dumps(2)),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert json.loads(out[0].content.as_text()) == [1, 2]


def test_flatten_default_off_keeps_nested_lists() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps([1, 2])),
                ip_message(json.dumps(3)),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert json.loads(out[0].content.as_text()) == [[1, 2], 3]


def test_flatten_one_level_splices_top_level_lists() -> None:
    component = Component(METADATA)
    component.apply_config_values({"flatten": True})

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps([1, 2])),
                ip_message(json.dumps([3, [4, 5]])),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert json.loads(out[0].content.as_text()) == [1, 2, 3, [4, 5]]


def test_flatten_two_levels_unwraps_further() -> None:
    component = Component(METADATA)
    component.apply_config_values({"flatten": True, "flatten_levels": 2})

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(json.dumps([1, 2])),
                ip_message(json.dumps([3, [4, 5]])),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert json.loads(out[0].content.as_text()) == [1, 2, 3, 4, 5]


def test_merges_attributes_in_ip_order_with_later_overwriting_earlier() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message_with_attrs(json.dumps(1), a="first", b="keep"),
                ip_message_with_attrs(json.dumps(2), a="second"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out_ip = result.output("out").values[0]
    merged = {kv.key: kv.value.as_text() for kv in out_ip.attributes}
    assert merged["a"] == "second"
    assert merged["b"] == "keep"


def test_remove_attrs_drops_named_attributes_from_the_merge() -> None:
    component = Component(METADATA)
    component.apply_config_values({"remove_attrs": ["secret"]})

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message_with_attrs(json.dumps(1), secret="hidden", keep="visible"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out_ip = result.output("out").values[0]
    attr_names = [kv.key for kv in out_ip.attributes]
    assert "secret" not in attr_names
    assert "keep" in attr_names
