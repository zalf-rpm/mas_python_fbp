from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import templating
from zalfmas_fbp.components.common.templating import TemplateError, render
from zalfmas_fbp.components.common.values import VALUE_TYPE


def ip(content="", content_type=None, typed=True, **attrs):
    message = fbp_capnp.IP.new_message(content=content)
    if content_type:
        message.sysAttributes.contentType = content_type
    if attrs:
        kvs = message.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            kvs[i].value = (
                common_capnp.Value.new_message(t=value)
                if isinstance(value, str)
                else common_capnp.Value.new_message(f64=float(value))
            )
            if typed:
                kvs[i].valueType = VALUE_TYPE
    return message.as_reader()


# --- placeholder forms ------------------------------------------------------------------------


def test_attribute_placeholder() -> None:
    assert render("{@region}", ip(region="north")) == "north"


def test_content_and_content_path_placeholders() -> None:
    assert render("{.}", ip("plain")) == "plain"
    assert render("{./site/id}", ip(json.dumps({"site": {"id": 42}}))) == "42"


def test_metadata_placeholder() -> None:
    assert render("{#type}", ip("x")) == "standard"


def test_count_placeholder() -> None:
    assert render("file_{count}.csv", ip(), count=7) == "file_7.csv"


def test_now_placeholder() -> None:
    when = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
    assert render("{now:%Y-%m-%d}", ip(), now=when) == "2026-09-29"
    assert render("{now}", ip(), now=when).startswith("2026-09-29T12:00")


def test_format_specs_work_as_in_str_format() -> None:
    assert render("{count:03d}", ip(), count=7) == "007"
    assert render("{@v:.2f}", ip(v=3.14159)) == "3.14"


def test_number_format_applies_where_no_spec_is_given() -> None:
    assert render("{@v}", ip(v=3.14159), number_format=".1f") == "3.1"
    assert render("{@v:.3f}", ip(v=3.14159), number_format=".1f") == "3.142"


def test_number_format_leaves_strings_alone() -> None:
    assert render("{@region}", ip(region="north"), number_format=".2f") == "north"


def test_doubled_braces_are_literal() -> None:
    assert render("{{not a placeholder}} {@region}", ip(region="north")) == "{not a placeholder} north"


def test_several_placeholders_in_one_pattern() -> None:
    result = render("{@region}_{count:02d}.csv", ip(region="north"), count=3)
    assert result == "north_03.csv"


# --- validation and missing values ----------------------------------------------------------


@pytest.mark.parametrize("pattern", ["{region}", "{attr_name}", "{}"])
def test_a_placeholder_without_a_sigil_is_rejected(pattern) -> None:
    """A typo should be reported, not rendered as itself."""
    with pytest.raises(TemplateError, match="not a usable placeholder"):
        templating.validate_pattern(pattern)


def test_reserved_names_need_no_sigil() -> None:
    templating.validate_pattern("{count} {now}")


@pytest.mark.parametrize(
    ("missing", "expected"),
    [("empty", ""), ("keep", "{@nope}")],
)
def test_missing_policies(missing, expected) -> None:
    assert render("{@nope}", ip(), missing=missing) == expected


def test_missing_error_raises() -> None:
    with pytest.raises(TemplateError, match="could not be resolved"):
        render("{@nope}", ip(), missing="error")


def test_placeholders_in_reports_what_a_pattern_uses() -> None:
    assert templating.placeholders_in("{@a}_{count}_{{x}}") == ["@a", "count"]


# --- attribute typing -------------------------------------------------------------------------


def test_an_untyped_value_attribute_is_missing_without_a_declared_type() -> None:
    """D14: reading it would mean guessing the schema."""
    assert render("{@v}", ip(v=42, typed=False), missing="empty") == ""


def test_a_declared_attribute_type_makes_it_readable() -> None:
    assert render("{@v}", ip(v=42, typed=False), attr_types={"v": VALUE_TYPE}) == "42.0"


def test_the_wildcard_declares_a_type_for_every_untyped_attribute() -> None:
    assert render("{@v}", ip(v=42, typed=False), attr_types={"*": VALUE_TYPE}) == "42.0"


def test_a_named_type_wins_over_the_wildcard() -> None:
    rendered = render("{@v}", ip(v=42, typed=False), attr_types={"v": VALUE_TYPE, "*": "nonsense"})
    assert rendered == "42.0"
