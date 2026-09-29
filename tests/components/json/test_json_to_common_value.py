"""Characterization tests for json/json_to_common_value.

Written against the pre-refactor implementation to pin its behaviour before the type-selection
logic moved into components/common/values.py (plan D12). They assert on the exact Value union field
chosen, which is the part being extracted, and deliberately avoid using values.py to read results
so they stay an independent check on the refactor.
"""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import (
    done_message,
    ip_message,
    ip_message_with_attrs,
    run_process_component,
)
from zalfmas_fbp.components.json.json_to_common_value import METADATA, JsonToCommonValue


def run(payload, conf: dict | None = None, inputs=None):
    component = JsonToCommonValue(METADATA)
    ports: dict = {"in": [ip_message(json.dumps(payload)), done_message()]}
    if inputs is not None:
        ports = dict(inputs)
    if conf:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(conf))),
            done_message(),
        ]
    return run_process_component(component, inputs=ports).output()


def values_of(writer):
    return [value.content.as_struct(common_capnp.Value) for value in writer.values]


def only(writer):
    assert len(writer.values) == 1
    return values_of(writer)[0]


# --- automatic type selection ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "field", "expected"),
    [
        (42, "ui8", 42),
        (300, "ui16", 300),
        (-1, "i8", -1),
        (1.5, "f32", 1.5),
        (True, "b", True),
        ("text", "t", "text"),
    ],
)
def test_scalars_get_the_smallest_fitting_field(payload, field, expected) -> None:
    result = only(run(payload))
    assert result.which() == field
    assert getattr(result, field) == expected


def test_smallest_type_optimization_can_be_turned_off() -> None:
    assert only(run(42, {"optimize_smallest_type": False})).which() == "ui64"


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        ([1, 2, 3], "lui8"),
        ([200], "lui8"),
        ([300], "lui16"),
        ([-1, 2], "li8"),
        ([1.5], "lf32"),
        ([True, False], "lb"),
        (["a"], "lt"),
    ],
)
def test_lists_get_the_smallest_fitting_list_field(payload, field) -> None:
    """Lists use the same unsigned-first ordering as scalars, so 200 is ui8 alone and lui8 in a list.

    Changed deliberately in D15: before the D12 refactor lists tried signed fields first, making
    [200] an li16 while the bare 200 was a ui8.
    """
    assert only(run(payload)).which() == field


def test_empty_lists_become_lf64() -> None:
    assert only(run([])).which() == "lf64"


def test_objects_become_lpair() -> None:
    result = only(run({"a": 1, "b": "x"}))
    assert result.which() == "lpair"
    assert [pair.fst.as_text() for pair in result.lpair] == ["a", "b"]


def test_nested_objects_recurse() -> None:
    result = only(run({"outer": {"inner": 5}}))
    inner = result.lpair[0].snd.as_struct(common_capnp.Value)
    assert inner.which() == "lpair"
    assert inner.lpair[0].fst.as_text() == "inner"


# --- requested types ------------------------------------------------------------------------


def test_requested_type_is_used_when_it_fits() -> None:
    assert only(run(42, {"requested_type": "i64"})).which() == "i64"


def test_requested_type_falls_back_when_it_does_not_fit() -> None:
    assert only(run(70000, {"requested_type": "ui8"})).which() == "ui32"


def test_a_failing_requested_type_without_fallback_skips_the_message() -> None:
    writer = run(70000, {"requested_type": "ui8", "allow_fallback_if_requested_type_fails": False})
    assert writer.values == []


def test_an_object_forces_lpair_even_when_another_type_is_requested() -> None:
    assert only(run({"a": 1}, {"requested_type": "i64"})).which() == "lpair"


# --- traversal ------------------------------------------------------------------------------


def test_traversal_path_selects_a_sub_value() -> None:
    result = only(run({"a": {"b": [7, 8]}}, {"traversal_path": "a/b"}))
    assert result.which() == "lui8"
    assert list(result.lui8) == [7, 8]


def test_traversal_path_indexes_lists() -> None:
    assert only(run({"a": [10, 20]}, {"traversal_path": "a/1"})).ui8 == 20


def test_an_unresolvable_traversal_path_skips_the_message() -> None:
    assert run({"a": 1}, {"traversal_path": "nope"}).values == []


def test_path_separator_is_configurable() -> None:
    assert only(run({"a": {"b": 3}}, {"traversal_path": "a.b", "path_separator": "."})).ui8 == 3


# --- sentinels ------------------------------------------------------------------------------


def test_json_null_without_a_sentinel_skips_the_message() -> None:
    assert run({"a": None}, {}).values == []


def test_a_null_sentinel_replaces_nulls_and_is_reported_as_an_attribute() -> None:
    writer = run([1, None, 3], {"null_sentinel": -9999})
    result = only(writer)
    assert list(result.li16) == [1, -9999, 3]  # negative sentinel forces a signed field
    assert [kv.key for kv in writer.values[0].attributes] == ["null_sentinel"]


def test_the_sentinel_attribute_is_attached_whenever_a_sentinel_is_configured() -> None:
    """Even when the payload contains no null at all - pinning existing behaviour."""
    writer = run([1, 2], {"null_sentinel": -1})
    assert [kv.key for kv in writer.values[0].attributes] == ["null_sentinel"]


def test_sentinel_attributes_can_be_suppressed() -> None:
    writer = run([1, None], {"null_sentinel": -1, "attach_sentinel_attributes": False})
    assert list(writer.values[0].attributes) == []


def test_sentinel_attribute_names_are_configurable() -> None:
    writer = run([1, None], {"null_sentinel": -1, "null_sentinel_attr": "nulls_were"})
    assert [kv.key for kv in writer.values[0].attributes] == ["nulls_were"]


# --- errors and stream handling ---------------------------------------------------------------


def test_invalid_json_is_skipped_by_default() -> None:
    component = JsonToCommonValue(METADATA)
    writer = run_process_component(
        component,
        inputs={"in": [ip_message("{not json"), ip_message("5"), done_message()]},
    ).output()
    assert [value.content.as_struct(common_capnp.Value).ui8 for value in writer.values] == [5]


def test_skip_on_error_off_emits_an_empty_text_value() -> None:
    component = JsonToCommonValue(METADATA)
    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(
                    common_capnp.StructuredText.new_message(type="json", value='{"skip_on_error": false}'),
                ),
                done_message(),
            ],
            "in": [ip_message("{not json"), done_message()],
        },
    ).output()
    result = only(writer)
    assert (result.which(), result.t) == ("t", "")


def test_bracket_ips_are_forwarded_unchanged() -> None:
    component = JsonToCommonValue(METADATA)
    writer = run_process_component(
        component,
        inputs={
            "in": [
                PortMessage_open(),
                ip_message("1"),
                PortMessage_close(),
                done_message(),
            ],
        },
    ).output()
    assert [str(value.type) for value in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_incoming_attributes_are_propagated() -> None:
    component = JsonToCommonValue(METADATA)
    writer = run_process_component(
        component,
        inputs={"in": [ip_message_with_attrs("1", source="upstream"), done_message()]},
    ).output()
    assert [(kv.key, kv.value.as_text()) for kv in writer.values[0].attributes] == [("source", "upstream")]


def PortMessage_open():
    from tests.component_harness import open_bracket_message

    return open_bracket_message()


def PortMessage_close():
    from tests.component_harness import close_bracket_message

    return close_bracket_message()


def test_a_configured_sentinel_widens_the_payload_type_to_hold_it() -> None:
    """D15: the sentinel belongs to the domain even when this message has no nulls, so the type
    must accommodate it. Without that, a stream flaps between lui8 and li16 message to message.
    """
    writer = run([1, 2], {"null_sentinel": -1})
    result = only(writer)
    assert result.which() == "li8"
    assert list(result.li8) == [1, 2]

    sentinel = writer.values[0].attributes[0]
    assert sentinel.key == "null_sentinel"
    assert sentinel.value.as_struct(common_capnp.Value).which() == "i8"


def test_a_message_with_nulls_and_one_without_agree_on_the_type() -> None:
    """The point of the rule: both shapes of message are typed the same way."""
    without_nulls = only(run([1, 2], {"null_sentinel": -9999}))
    with_nulls = only(run([1, None, 2], {"null_sentinel": -9999}))
    assert without_nulls.which() == with_nulls.which() == "li16"


def test_a_positive_sentinel_does_not_force_a_signed_type() -> None:
    """It is not 'sentinel means signed' - the sentinel is just another value the type must hold."""
    assert only(run([1, 2], {"null_sentinel": 999})).which() == "lui16"


def test_a_sentinel_of_another_kind_widens_a_list_to_boxed_values() -> None:
    """Same result as if the sentinel had actually been substituted into the list."""
    assert only(run([1, 2], {"null_sentinel": "N/A"})).which() == "lv"
    assert only(run([1, None], {"null_sentinel": "N/A"})).which() == "lv"


def test_a_sentinel_that_cannot_be_accommodated_falls_back_to_its_own_type() -> None:
    """A text scalar with a numeric sentinel: no single Value field holds both, so the attribute
    takes its own type rather than the message being dropped.
    """
    writer = run("hello", {"null_sentinel": -1})
    assert only(writer).which() == "t"

    sentinel = writer.values[0].attributes[0]
    assert sentinel.value.as_struct(common_capnp.Value).i8 == -1
