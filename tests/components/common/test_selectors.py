from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import selectors
from zalfmas_fbp.components.common.selectors import MISSING, Op, Predicate, parse_selector, resolve
from zalfmas_fbp.components.common.values import VALUE_TYPE


def _ip(content=None, ip_type="standard", content_type: str | None = None, **attrs):
    ip = fbp_capnp.IP.new_message(type=ip_type)
    if content is not None:
        ip.content = content
    if content_type:
        ip.sysAttributes.contentType = content_type
    if attrs:
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            if isinstance(value, str):
                kvs[i].value = value
            else:
                kvs[i].value = value
                kvs[i].valueType = VALUE_TYPE
    return ip.as_reader()


def _json_ip(payload, **attrs):
    return _ip(content=json.dumps(payload), **attrs)


# --- parsing -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("spec", "kind", "name", "path"),
    [
        ("@region", selectors.SelectorKind.ATTRIBUTE, "region", ()),
        ("@a/b/0", selectors.SelectorKind.ATTRIBUTE, "a", ("b", 0)),
        (".", selectors.SelectorKind.CONTENT, "", ()),
        ("./a/b", selectors.SelectorKind.CONTENT, "", ("a", "b")),
        ("./items/0", selectors.SelectorKind.CONTENT, "", ("items", 0)),
        ("./items/-1", selectors.SelectorKind.CONTENT, "", ("items", -1)),
        ("#type", selectors.SelectorKind.META, "type", ()),
    ],
)
def test_sigils_parse_to_selectors(spec, kind, name, path) -> None:
    selector = parse_selector(spec)
    assert (selector.kind, selector.name, selector.path) == (kind, name, path)


@pytest.mark.parametrize("spec", ["brandenburg", "", "a/b", "hello.world", "user@host"])
def test_unsigiled_strings_are_literals(spec) -> None:
    assert parse_selector(spec).literal == spec


@pytest.mark.parametrize("spec", [42, 1.5, True, None, ["a"]])
def test_non_strings_are_always_literals(spec) -> None:
    selector = parse_selector(spec)
    assert selector.is_literal
    assert selector.literal == spec


@pytest.mark.parametrize(("spec", "expected"), [(r"\@zalf.de", "@zalf.de"), (r"\.gitignore", ".gitignore")])
def test_a_leading_backslash_escapes_a_sigil(spec, expected) -> None:
    """D1: a literal that really starts with a sigil is escaped."""
    selector = parse_selector(spec)
    assert selector.is_literal
    assert selector.literal == expected


def test_gjson_prefix_is_opt_in() -> None:
    selector = parse_selector("gjson:a.b.1")
    assert selector.kind is selectors.SelectorKind.GJSON
    assert selector.query == "a.b.1"


def test_path_separator_is_configurable() -> None:
    assert parse_selector("@a.b.0", separator=".").path == ("b", 0)


# --- resolution ----------------------------------------------------------------------------


def test_resolves_typed_and_raw_attributes() -> None:
    ip = _ip(num=common_capnp.Value.new_message(i64=42), raw="text")
    assert resolve(ip, "@num") == 42
    assert resolve(ip, "@raw") == "text"
    assert resolve(ip, "@nope") is MISSING


def test_resolves_a_path_into_an_attribute_value() -> None:
    nested = common_capnp.Value.new_message(
        lpair=[common_capnp.Pair.new_message(fst="sub", snd=common_capnp.Value.new_message(t="deep"))],
    )
    assert resolve(_ip(obj=nested), "@obj/sub") == "deep"


def test_resolves_content_and_paths_into_json_content() -> None:
    ip = _json_ip({"a": {"b": [10, 20]}})
    assert resolve(ip, "./a/b/1") == 20
    assert resolve(ip, "./a/b/-1") == 20
    assert resolve(ip, "./a/missing") is MISSING
    assert json.loads(resolve(ip, ".")) == {"a": {"b": [10, 20]}}


def test_integer_segments_index_lists_but_can_still_name_object_keys() -> None:
    """D11: digits are indices, with a fallback for JSON objects keyed by digit strings."""
    assert resolve(_json_ip([{"x": 1}, {"x": 2}]), "./1/x") == 2
    assert resolve(_json_ip({"0": "zero"}), "./0") == "zero"


def test_indexing_past_the_end_or_into_a_scalar_is_missing() -> None:
    assert resolve(_json_ip([1, 2]), "./5") is MISSING
    assert resolve(_json_ip({"a": 1}), "./a/b") is MISSING


def test_resolves_ip_metadata() -> None:
    assert resolve(_ip(content="x"), "#type") == "standard"
    assert resolve(_ip(ip_type="openBracket"), "#type") == "openBracket"
    assert resolve(_ip(content="x"), "#contentType") is MISSING
    assert resolve(_ip(content="x", content_type="Text"), "#contentType") == "Text"
    assert resolve(_ip(content="x"), "#nonsense") is MISSING


def test_resolves_a_gjson_query() -> None:
    assert resolve(_json_ip({"a": {"b": [1, 2, 3]}}), "gjson:a.b.1") == 2
    assert resolve(_json_ip({"a": 1}), "gjson:nope") is MISSING


def test_literals_resolve_to_themselves() -> None:
    assert resolve(_ip(content="x"), "brandenburg") == "brandenburg"
    assert resolve(_ip(content="x"), 42) == 42


def test_attr_types_resolve_attributes_written_without_a_value_type() -> None:
    ip = fbp_capnp.IP.new_message()
    kvs = ip.init("attributes", 1)
    kvs[0].key = "untyped"
    kvs[0].value = common_capnp.Value.new_message(i64=5)
    reader = ip.as_reader()

    assert resolve(reader, "@untyped") is MISSING
    assert resolve(reader, "@untyped", attr_types={"untyped": VALUE_TYPE}) == 5


# --- comparison ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("left", "right", "op", "expected"),
    [
        (2020, "2020", Op.EQ, True),
        ("2020", 2020, Op.EQ, True),
        (1.0, 1, Op.EQ, True),
        (5, "10", Op.LT, True),
        ("abc", "abd", Op.LT, True),
        (True, "true", Op.EQ, True),
        (False, "1", Op.EQ, False),
    ],
)
def test_comparisons_coerce_permissively(left, right, op, expected) -> None:
    """D8: TOML cannot express capnp numeric types, so coerce by default."""
    assert selectors.compare(left, right, op) is expected


def test_strict_types_disables_coercion() -> None:
    assert selectors.compare(2020, "2020", Op.EQ) is True
    assert selectors.compare(2020, "2020", Op.EQ, strict_types=True) is False


def test_ordering_incomparable_values_is_false_not_an_error() -> None:
    assert selectors.compare(None, 1, Op.LT) is False


@pytest.mark.parametrize(
    ("left", "right", "op", "expected"),
    [
        ("b", ["a", "b"], Op.IN, True),
        ("c", ["a", "b"], Op.NOT_IN, True),
        (["a", "b"], "a", Op.CONTAINS, True),
        ("hello", "he", Op.STARTSWITH, True),
        ("hello", "lo", Op.ENDSWITH, True),
        ("hello", "^h.*o$", Op.MATCHES, True),
    ],
)
def test_membership_and_string_operators(left, right, op, expected) -> None:
    assert selectors.compare(left, right, op) is expected


def test_an_invalid_regex_is_false_rather_than_raising() -> None:
    assert selectors.compare("x", "[unclosed", Op.MATCHES) is False


# --- predicates ----------------------------------------------------------------------------


def test_leaf_predicate_against_an_attribute() -> None:
    ip = _ip(region="brandenburg")
    assert selectors.evaluate(ip, Predicate(left="@region", op=Op.EQ, right="brandenburg"))
    assert not selectors.evaluate(ip, Predicate(left="@region", op=Op.EQ, right="saxony"))


def test_predicate_against_a_json_content_path() -> None:
    ip = _json_ip({"yield": 7.1})
    assert selectors.evaluate(ip, Predicate(left="./yield", op=Op.GT, right=5.0))
    assert not selectors.evaluate(ip, Predicate(left="./yield", op=Op.GT, right=9.0))


def test_attribute_to_attribute_comparison_needs_a_sigil_on_the_right() -> None:
    ip = _ip(a="x", b="x")
    assert selectors.evaluate(ip, Predicate(left="@a", op=Op.EQ, right="@b"))
    assert not selectors.evaluate(ip, Predicate(left="@a", op=Op.EQ, right="b"))


def test_unresolvable_selectors_make_a_leaf_false() -> None:
    ip = _ip(content="x")
    assert not selectors.evaluate(ip, Predicate(left="@nope", op=Op.EQ, right="x"))
    assert not selectors.evaluate(ip, Predicate(left="@nope", op=Op.NE, right="x"))


@pytest.mark.parametrize(
    ("op", "expected"),
    [(Op.EXISTS, True), (Op.THE_MISSING, False), (Op.TRUTHY, True)],
)
def test_unary_operators_on_a_present_attribute(op, expected) -> None:
    assert selectors.evaluate(_ip(a="value"), Predicate(left="@a", op=op)) is expected


def test_unary_operators_on_an_absent_attribute() -> None:
    ip = _ip(content="x")
    assert selectors.evaluate(ip, Predicate(left="@a", op=Op.THE_MISSING))
    assert not selectors.evaluate(ip, Predicate(left="@a", op=Op.EXISTS))
    assert not selectors.evaluate(ip, Predicate(left="@a", op=Op.TRUTHY))


def test_is_null_distinguishes_json_null_from_absent() -> None:
    assert selectors.evaluate(_json_ip({"a": None}), Predicate(left="./a", op=Op.IS_NULL))
    assert not selectors.evaluate(_json_ip({"a": 1}), Predicate(left="./a", op=Op.IS_NULL))
    assert not selectors.evaluate(_json_ip({}), Predicate(left="./a", op=Op.IS_NULL))


def test_combinators_nest() -> None:
    ip = _json_ip({"yield": 7.1}, region="brandenburg")
    matching = Predicate.model_validate(
        {
            "all": [
                {"left": "@region", "op": "eq", "right": "brandenburg"},
                {"any": [{"left": "./yield", "op": "gt", "right": 9.0}, {"left": "./yield", "op": "gt", "right": 5.0}]},
                {"not": {"left": "@region", "op": "eq", "right": "saxony"}},
            ],
        },
    )
    assert selectors.evaluate(ip, matching)


def test_predicates_load_from_toml_shaped_config() -> None:
    predicate = Predicate.model_validate({"left": "@region", "op": "eq", "right": "brandenburg"})
    assert predicate.op is Op.EQ
    assert selectors.evaluate(_ip(region="brandenburg"), predicate)


@pytest.mark.parametrize(
    "payload",
    [
        {"all": [{"left": "@a"}], "any": [{"left": "@b"}]},
        {"all": [{"left": "@a"}], "left": "@b"},
        {"op": "eq", "right": "x"},
    ],
)
def test_malformed_predicates_are_rejected(payload) -> None:
    with pytest.raises(ValueError, match="predicate"):
        Predicate.model_validate(payload)
