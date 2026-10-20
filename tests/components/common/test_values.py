from __future__ import annotations

import json

import capnp
import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import values
from zalfmas_fbp.components.common.values import MISSING


def _ip(content=None, attrs: list[tuple[str, object, str | None]] | None = None, content_type: str | None = None):
    ip = fbp_capnp.IP.new_message()
    if content is not None:
        ip.content = content
    if content_type:
        ip.sysAttributes.contentType = content_type
    if attrs:
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value, value_type) in enumerate(attrs):
            kvs[i].key = key
            kvs[i].value = value
            if value_type:
                kvs[i].valueType = value_type
    return ip.as_reader()


def test_missing_sentinel_is_falsy_and_singleton() -> None:
    assert not MISSING
    assert values._Missing() is MISSING
    assert repr(MISSING) == "MISSING"


@pytest.mark.parametrize(
    ("field", "payload", "expected"),
    [
        ("i64", 42, 42),
        ("f64", 1.5, 1.5),
        ("b", True, True),
        ("t", "hello", "hello"),
        ("li64", [1, 2, 3], [1, 2, 3]),
        ("lt", ["a", "b"], ["a", "b"]),
        ("lb", [True, False], [True, False]),
    ],
)
def test_python_from_value_scalars_and_lists(field, payload, expected) -> None:
    reader = common_capnp.Value.new_message(**{field: payload}).as_reader()
    assert values.python_from_value(reader) == expected


def test_python_from_value_data_becomes_bytes() -> None:
    reader = common_capnp.Value.new_message(d=b"\x01\x02").as_reader()
    assert values.python_from_value(reader) == b"\x01\x02"


def test_python_from_value_lpair_becomes_dict_and_lv_a_list() -> None:
    pairs = common_capnp.Value.new_message(
        lpair=[
            common_capnp.Pair.new_message(fst="a", snd=common_capnp.Value.new_message(t="x")),
            common_capnp.Pair.new_message(fst="n", snd=common_capnp.Value.new_message(i64=7)),
        ],
    ).as_reader()
    assert values.python_from_value(pairs) == {"a": "x", "n": 7}

    nested = common_capnp.Value.new_message(
        lv=[common_capnp.Value.new_message(b=True), common_capnp.Value.new_message(i64=2)],
    ).as_reader()
    assert values.python_from_value(nested) == [True, 2]


def test_python_from_structured_text_parses_by_tag() -> None:
    as_json = common_capnp.StructuredText.new_message(type="json", value='{"a": 1}').as_reader()
    assert values.python_from_structured_text(as_json) == {"a": 1}

    as_toml = common_capnp.StructuredText.new_message(type="toml", value="a = 1").as_reader()
    assert values.python_from_structured_text(as_toml) == {"a": 1}


def test_python_from_structured_text_returns_raw_text_when_unparseable() -> None:
    broken = common_capnp.StructuredText.new_message(type="json", value="{not json").as_reader()
    assert values.python_from_structured_text(broken) == "{not json"


def test_python_from_attr_uses_value_type_when_set() -> None:
    ip = _ip(attrs=[("num", common_capnp.Value.new_message(i64=42), values.VALUE_TYPE)])
    assert values.python_from_attr(values.attr_reader(ip, "num")) == 42


def test_python_from_attr_reads_raw_text_attributes() -> None:
    """D4: older components write plain strings, which must keep working."""
    ip = _ip(attrs=[("raw", "hello", None)])
    assert values.python_from_attr(values.attr_reader(ip, "raw")) == "hello"


def test_python_from_attr_refuses_to_guess_an_untyped_struct() -> None:
    """A struct-to-struct cast silently misreads, so an untyped struct must resolve to MISSING."""
    ip = _ip(attrs=[("obj", common_capnp.Value.new_message(i64=42), None)])
    assert values.python_from_attr(values.attr_reader(ip, "obj")) is MISSING


def test_python_from_attr_accepts_a_type_hint_for_untyped_structs() -> None:
    ip = _ip(attrs=[("obj", common_capnp.Value.new_message(i64=42), None)])
    assert values.python_from_attr(values.attr_reader(ip, "obj"), values.VALUE_TYPE) == 42


def test_untyped_content_falls_back_to_text_only() -> None:
    assert values.python_from_content(_ip(content="plain")) == "plain"
    assert values.python_from_content(_ip(content=common_capnp.Value.new_message(i64=1))) is MISSING


def test_content_type_on_the_ip_wins_over_the_configured_fallback() -> None:
    ip = _ip(content=common_capnp.Value.new_message(i64=7), content_type=values.VALUE_TYPE)
    assert values.python_from_content(ip, values.STRUCTURED_TEXT_TYPE) == 7


def test_configured_content_type_is_used_when_the_ip_carries_none() -> None:
    ip = _ip(content=common_capnp.StructuredText.new_message(type="json", value=json.dumps({"a": 1})))
    assert values.python_from_content(ip, values.STRUCTURED_TEXT_TYPE) == {"a": 1}


def test_resolve_schema_caches_hits_and_misses() -> None:
    assert values.resolve_schema(values.VALUE_TYPE) is not None
    assert values.resolve_schema("not a content type") is None
    assert values.resolve_schema(None) is None
    assert "not a content type" in values._schema_cache


# --- D14: why reads must be type-driven ----------------------------------------------------


def test_wrong_struct_schema_cast_is_silent_not_an_error() -> None:
    """Pins the pycapnp behaviour that forces D14 (type-driven reads, never guessing).

    Cap'n Proto validates a pointer's *kind*, not which struct it holds, and reads past the end of
    the allocated data section return defaults. So reading a StructuredText as a Value yields the
    f64 branch holding the `json` enumerant (1) reinterpreted as a double, rather than raising.

    If this ever starts raising, the fallback chains in this module could be simplified.
    """
    ip = fbp_capnp.IP.new_message(
        content=common_capnp.StructuredText.new_message(type="json", value='{"a":1}'),
    )
    misread = ip.as_reader().content.as_struct(common_capnp.Value)

    assert misread.which() == "f64"
    assert misread.f64 == 5e-324


def test_text_and_struct_pointer_kinds_are_distinguishable() -> None:
    """The one discrimination that *is* sound, and which python_from_attr relies on."""
    text_pointer = fbp_capnp.IP.new_message(content="plain text").as_reader().content
    struct_pointer = (
        fbp_capnp.IP.new_message(
            content=common_capnp.StructuredText.new_message(type="json", value="{}"),
        )
        .as_reader()
        .content
    )

    assert text_pointer.as_text() == "plain text"
    with pytest.raises(capnp.KjException):
        _ = text_pointer.as_struct(common_capnp.Value)
    with pytest.raises(capnp.KjException):
        _ = struct_pointer.as_text()
