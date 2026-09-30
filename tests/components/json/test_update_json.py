from __future__ import annotations

import json

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.json.update_json import as_type, read_attr_value


def _attrs_from(**attrs) -> dict:
    """Build a real IP, read it back, and return {key: value} the way read_attr_value expects -
    same shape as update_json.py's own `{kv.key: kv.value for kv in in_ip.attributes}`.
    """
    ip = fbp_capnp.IP.new_message()
    entries = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        entries[i].key = key
        entries[i].value = value
    ip_r = ip.as_reader()
    return {kv.key: kv.value for kv in ip_r.attributes}


def test_as_type_json_pseudo_type_accepts_an_already_plain_str() -> None:
    # a prior JSON decode step (e.g. dict access into an already-decoded sub-object) can hand
    # as_type a plain Python str rather than a capnp Text value - both must work.
    value, schema = as_type(json.dumps("already a plain str"), "JSON")
    assert value == "already a plain str"
    assert schema is None


def test_as_type_json_pseudo_type_is_case_insensitive() -> None:
    value, schema = as_type(json.dumps({"a": 1}), "json")
    assert value == {"a": 1}
    assert schema is None


def test_read_attr_value_json_pseudo_type_on_plain_text_attribute() -> None:
    attrs = _attrs_from(myattr=json.dumps({"a": {"b": 42}}))
    types = {"@myattr": "JSON"}

    result, success = read_attr_value(types, attrs, ["@myattr", "a", "b"])

    assert success is True
    assert result == 42


def test_read_attr_value_json_pseudo_type_whole_attribute_no_drill() -> None:
    attrs = _attrs_from(myattr=json.dumps([1, 2, 3]))
    types = {"@myattr": "JSON"}

    result, success = read_attr_value(types, attrs, ["@myattr"])

    assert success is True
    assert result == [1, 2, 3]


def test_read_attr_value_per_segment_json_pseudo_type_recasts_anypointer_field() -> None:
    # Pair's fields are generic (AnyPointer at the dynamic-access level): reaching 'fst' alone isn't
    # enough to json.loads() it - the ':@myjson' suffix must recast it via the JSON pseudo-type first.
    pair = common_capnp.Pair.new_message(fst=json.dumps({"x": {"y": 7}}), snd="ignored")
    attrs = _attrs_from(mypair=pair)
    types = {
        "@mypair": "@0xb9d4864725174733 = common/common.capnp:Pair",
        "@myjson": "JSON",
    }

    result, success = read_attr_value(types, attrs, ["@mypair", "fst:@myjson", "x", "y"])

    assert success is True
    assert result == 7


def test_read_attr_value_still_auto_decodes_structured_text_json() -> None:
    # regression check: the pre-existing common.capnp:StructuredText[JSON] auto-decode must keep
    # working after replacing the is_json flag with a direct isinstance(attr_val, dict) check.
    st = common_capnp.StructuredText.new_message(type="json", value=json.dumps({"a": {"b": 5}}))
    attrs = _attrs_from(myattr=st)
    types = {"@myattr": "@0xed6c098b67cad454 = common/common.capnp:StructuredText"}

    result, success = read_attr_value(types, attrs, ["@myattr", "a", "b"])

    assert success is True
    assert result == 5


def test_read_attr_value_structured_text_value_segment_returns_raw_string() -> None:
    # regression check: asking for the raw 'value' field itself must still skip auto-decoding.
    st = common_capnp.StructuredText.new_message(type="json", value=json.dumps({"a": 1}))
    attrs = _attrs_from(myattr=st)
    types = {"@myattr": "@0xed6c098b67cad454 = common/common.capnp:StructuredText"}

    result, success = read_attr_value(types, attrs, ["@myattr", "value"])

    assert success is True
    assert result == json.dumps({"a": 1})


# --- a list addressed by something that is not an index -----------------------------------------


def test_a_non_index_key_against_a_list_is_skipped_not_fatal(caplog) -> None:
    """It used to fall through to `json_obj["key"]` on a list and raise TypeError, which the
    caller caught as 'couldn't apply <op>' - abandoning the rest of the spec, with whatever had
    already been applied left in place."""

    from zalfmas_fbp.components.json.update_json import METADATA, UpdateJson

    component = UpdateJson(METADATA)
    data = [1, 2, 3]
    component.change(data, {"not_an_index": 9}, {})
    assert data == [1, 2, 3]
    assert "does not address an element of a list" in caplog.text


def test_the_rest_of_a_spec_still_applies_after_a_bad_key(caplog) -> None:
    from zalfmas_fbp.components.json.update_json import METADATA, UpdateJson

    component = UpdateJson(METADATA)
    data = [1, 2, 3]
    component.change(data, {"not_an_index": 9, "0": 42}, {}, allowed_operation="replace")
    assert data == [42, 2, 3]


def test_an_index_key_against_a_list_still_works() -> None:
    from zalfmas_fbp.components.json.update_json import METADATA, UpdateJson

    component = UpdateJson(METADATA)
    data = [1, 2, 3]
    component.change(data, {"1": 99}, {}, allowed_operation="replace")
    assert data == [1, 99, 3]


def test_a_dict_key_still_works() -> None:
    from zalfmas_fbp.components.json.update_json import METADATA, UpdateJson

    component = UpdateJson(METADATA)
    data = {"a": 1}
    component.change(data, {"a": 2}, {}, allowed_operation="replace")
    assert data == {"a": 2}
