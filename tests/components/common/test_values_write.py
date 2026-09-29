from __future__ import annotations

import base64

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import values
from zalfmas_fbp.components.common.values import MISSING

ST_SCHEMA = common_capnp.StructuredText.schema
VALUE_SCHEMA = common_capnp.Value.schema
IP_SCHEMA = fbp_capnp.IP.schema


def roundtrip(obj, **kwargs):
    return values.python_from_value(values.value_from_python(obj, **kwargs).as_reader())


# --- value_from_python ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("obj", "expected_field"),
    [(42, "ui8"), (-1, "i8"), (300, "ui16"), (1.5, "f32"), (True, "b"), ("x", "t"), (b"\x01", "d")],
)
def test_smallest_fitting_field_is_chosen(obj, expected_field) -> None:
    assert values.value_from_python(obj).as_reader().which() == expected_field


def test_widest_field_is_chosen_when_smallest_is_off() -> None:
    assert values.value_from_python(42, smallest=False).as_reader().which() == "ui64"


@pytest.mark.parametrize(
    "obj",
    [42, -1, 1.5, True, "text", b"\x01\x02", [1, 2, 3], [1.5, 2.5], ["a", "b"], [True, False], []],
)
def test_scalars_and_lists_round_trip(obj) -> None:
    assert roundtrip(obj) == obj


def test_dicts_round_trip_through_lpair() -> None:
    obj = {"a": 1, "b": "x", "c": [1, 2], "d": {"nested": True}}
    assert values.value_from_python(obj).as_reader().which() == "lpair"
    assert roundtrip(obj) == obj


def test_mixed_type_lists_fall_back_to_lv() -> None:
    obj = [1, "two", True]
    assert values.value_from_python(obj).as_reader().which() == "lv"
    assert roundtrip(obj) == obj


def test_mixed_int_and_float_lists_use_one_float_field() -> None:
    assert values.value_from_python([1, 2.5]).as_reader().which() == "lf32"
    assert roundtrip([1, 2.5]) == [1.0, 2.5]


def test_requested_type_is_honoured() -> None:
    assert values.value_from_python(42, requested_type="i64").as_reader().which() == "i64"
    assert values.value_from_python([1, 2], requested_type="lf64").as_reader().which() == "lf64"


def test_requested_type_falls_back_when_it_cannot_fit() -> None:
    assert values.value_from_python(70000, requested_type="ui8").as_reader().which() == "ui32"


def test_requested_type_raises_when_fallback_is_disabled() -> None:
    with pytest.raises((TypeError, ValueError)):
        values.value_from_python(70000, requested_type="ui8", allow_fallback=False)


def test_none_has_no_value_representation() -> None:
    with pytest.raises(ValueError, match="cannot represent None"):
        values.value_from_python(None)


def test_auto_select_can_be_disabled() -> None:
    with pytest.raises(ValueError, match="auto_select is disabled"):
        values.value_from_python(42, auto_select=False)


@pytest.mark.parametrize(
    ("value", "field"),
    [(200, "i8"), (-1, "ui8"), (True, "i64"), ("nan-ish", "f64"), ({"a": 1}, "t")],
)
def test_coerce_scalar_rejects_what_does_not_fit(value, field) -> None:
    with pytest.raises((TypeError, ValueError)):
        values.coerce_scalar_for_field(value, field)


@pytest.mark.parametrize(("text", "field", "expected"), [("5", "i64", 5), ("5.0", "i64", 5), ("1.5", "f64", 1.5)])
def test_coerce_scalar_accepts_numeric_text(text, field, expected) -> None:
    assert values.coerce_scalar_for_field(text, field) == expected


# --- json_from_capnp -----------------------------------------------------------------------


def test_json_from_capnp_converts_a_struct() -> None:
    reader = common_capnp.StructuredText.new_message(type="json", value="{}").as_reader()
    assert values.json_from_capnp(reader, ST_SCHEMA) == {"value": "{}", "type": "json"}


def test_json_from_capnp_special_cases_value_so_lpair_is_not_opaque() -> None:
    """to_dict() leaves lpair's generic fst/snd as opaque pointers, so Value needs its own path."""
    reader = values.value_from_python({"a": 1, "b": "x"}).as_reader()
    assert values.json_from_capnp(reader, VALUE_SCHEMA) == {"a": 1, "b": "x"}
    assert "fst" in str(reader.to_dict())  # the behaviour being worked around


@pytest.mark.parametrize(
    ("data_as", "expected"),
    [("base64", base64.b64encode(b"\x01\x02").decode()), ("hex", "0102"), ("list", [1, 2])],
)
def test_data_fields_are_encoded_for_json(data_as, expected) -> None:
    reader = values.value_from_python(b"\x01\x02").as_reader()
    assert values.json_from_capnp(reader, VALUE_SCHEMA, data_as=data_as) == expected


def test_unresolvable_pointers_follow_the_configured_policy() -> None:
    ip = fbp_capnp.IP.new_message()
    kvs = ip.init("attributes", 1)
    kvs[0].key = "k"
    kvs[0].value = common_capnp.Value.new_message(i64=1)  # untyped struct in an AnyPointer
    reader = ip.as_reader()

    dropped = values.json_from_capnp(reader, IP_SCHEMA)
    assert "value" not in dropped["attributes"][0]
    nulled = values.json_from_capnp(reader, IP_SCHEMA, unresolved="null")
    assert nulled["attributes"][0]["value"] is None


def test_text_in_an_any_pointer_is_still_recovered() -> None:
    ip = fbp_capnp.IP.new_message()
    kvs = ip.init("attributes", 1)
    kvs[0].key = "k"
    kvs[0].value = "plain"
    assert values.json_from_capnp(ip.as_reader(), IP_SCHEMA)["attributes"][0]["value"] == "plain"


# --- capnp_from_json -----------------------------------------------------------------------


def test_capnp_from_json_builds_a_struct() -> None:
    built = values.capnp_from_json({"type": "toml", "value": "a=1"}, ST_SCHEMA)
    assert (built.type, built.value) == ("toml", "a=1")


def test_capnp_from_json_works_from_a_resolved_schema() -> None:
    """resolve_schema returns a _StructSchema, which has no new_message - the common case."""
    schema = values.resolve_schema(values.STRUCTURED_TEXT_TYPE)
    assert values.capnp_from_json({"type": "json", "value": "{}"}, schema).value == "{}"


def test_capnp_from_json_coerces_numbers_and_text() -> None:
    assert values.capnp_from_json({"type": "json", "value": 5}, ST_SCHEMA).value == "5"


def test_coercion_can_be_disabled() -> None:
    with pytest.raises(ValueError, match="Could not build"):
        values.capnp_from_json({"type": "json", "value": 5}, ST_SCHEMA, coerce_numbers=False)


def test_unknown_fields_error_by_default_and_can_be_ignored() -> None:
    with pytest.raises(ValueError, match="has no field 'nope'"):
        values.capnp_from_json({"nope": 1}, ST_SCHEMA)
    built = values.capnp_from_json({"nope": 1, "value": "x"}, ST_SCHEMA, unknown_fields="ignore")
    assert built.value == "x"


def test_nested_structs_and_lists_are_built() -> None:
    built = values.capnp_from_json({"type": "standard", "attributes": [{"key": "k"}]}, IP_SCHEMA)
    assert built.attributes[0].key == "k"


def test_any_pointer_fields_give_a_clear_error_rather_than_a_kj_exception() -> None:
    with pytest.raises(ValueError, match="AnyPointer"):
        values.capnp_from_json({"attributes": [{"key": "k", "value": {"a": 1}}]}, IP_SCHEMA)


def test_any_pointer_fields_accept_text_and_skip_none() -> None:
    assert values.capnp_from_json({"attributes": [{"key": "k", "value": "txt"}]}, IP_SCHEMA) is not None
    assert values.capnp_from_json({"attributes": [{"key": "k", "value": None}]}, IP_SCHEMA) is not None


def test_a_value_target_is_delegated_to_value_from_python() -> None:
    built = values.capnp_from_json({"a": 1}, VALUE_SCHEMA)
    assert values.python_from_value(built.as_reader()) == {"a": 1}


def test_non_object_input_for_a_struct_target_is_rejected() -> None:
    with pytest.raises(TypeError, match="expected an object"):
        values.capnp_from_json([1, 2], ST_SCHEMA)


def test_struct_round_trips_through_json() -> None:
    original = {"type": "toml", "value": "a = 1"}
    built = values.capnp_from_json(original, ST_SCHEMA)
    assert values.json_from_capnp(built.as_reader(), ST_SCHEMA) == {"value": "a = 1", "type": "toml"}


def test_json_from_capnp_reports_missing_for_a_non_struct() -> None:
    assert values.json_from_capnp("not a reader", ST_SCHEMA) is MISSING


# --- must_accommodate (D15) -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "extra"),
    [([1, 2], -1), ([1, 2], -9999), ([1, 2], 0.5), ([1, 2], "N/A"), ([], -1), ([1, 2], 999)],
)
def test_accommodating_a_value_types_a_list_as_if_it_contained_it(payload, extra) -> None:
    """The point of the rule: a list that could contain the value is typed like one that does."""
    accommodated = values.value_from_python(payload, must_accommodate=[extra]).as_reader().which()
    actual = values.value_from_python([*payload, extra]).as_reader().which()
    assert accommodated == actual


def test_accommodation_reaches_every_leaf_of_a_nested_structure() -> None:
    """A null could appear anywhere, so every numeric leaf has to hold the sentinel."""
    built = values.value_from_python({"a": 1, "b": {"c": 2}}, must_accommodate=[-1]).as_reader()
    outer = built.lpair[0].snd.as_struct(VALUE_SCHEMA)
    inner = built.lpair[1].snd.as_struct(VALUE_SCHEMA).lpair[0].snd.as_struct(VALUE_SCHEMA)
    assert outer.which() == "i8"
    assert inner.which() == "i8"


def test_accommodation_widens_scalars_too() -> None:
    assert values.value_from_python(42).as_reader().which() == "ui8"
    assert values.value_from_python(42, must_accommodate=[-1]).as_reader().which() == "i8"


def test_a_non_numeric_extra_cannot_widen_a_scalar_and_is_ignored() -> None:
    """No single Value field holds both text and a number; the caller handles that case."""
    assert values.value_from_python("hello", must_accommodate=[-1]).as_reader().which() == "t"


def test_accommodation_does_not_change_values_only_the_field() -> None:
    built = values.value_from_python([1, 2], must_accommodate=[-9999]).as_reader()
    assert values.python_from_value(built) == [1, 2]


def test_a_content_type_naming_a_file_rather_than_a_struct_is_an_actionable_error() -> None:
    """The file id of fbp.capnp resolves to the file's schema, which pycapnp then fails to cast
    with a message that says nothing useful. Easy mistake: the file id sits at the top of the file.
    """
    file_schema = values.resolve_schema("@0xbf602c4868dbb22f = fbp/fbp.capnp")
    with pytest.raises(TypeError, match="is not a struct type"):
        values.capnp_from_json({"a": 1}, file_schema)
