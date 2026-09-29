from __future__ import annotations

import json

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.string.format_string import METADATA, FormatString


def attr_ip(content="", **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(FormatString(METADATA), inputs=ports, outputs=("out",)).output()


def texts(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def test_renders_into_the_content() -> None:
    writer = run([attr_ip("", region="north")], pattern="site: {@region}")
    assert texts(writer) == ["site: north"]


def test_the_running_count_is_the_ips_position() -> None:
    writer = run([ip_message("a"), ip_message("b")], pattern="{count}")
    assert texts(writer) == ["0", "1"]


def test_renders_from_a_json_content_path() -> None:
    writer = run([ip_message(json.dumps({"site": {"id": 42}}))], pattern="site-{./site/id}")
    assert texts(writer) == ["site-42"]


def test_can_write_to_an_attribute_and_keep_the_content() -> None:
    writer = run([attr_ip("payload", region="north")], pattern="{@region}-tag", to_attr="label")
    assert texts(writer) == ["payload"]
    attrs = {kv.key: python_from_attr(kv) for kv in writer.values[0].attributes}
    assert attrs["label"] == "north-tag"
    assert attrs["region"] == "north"


def test_attributes_are_preserved_when_rendering_into_the_content() -> None:
    writer = run([attr_ip("", region="north")], pattern="x")
    assert [kv.key for kv in writer.values[0].attributes] == ["region"]


def test_a_missing_placeholder_renders_empty_by_default() -> None:
    writer = run([ip_message("a")], pattern="[{@nope}]")
    assert texts(writer) == ["[]"]


def test_a_missing_placeholder_can_skip_the_ip() -> None:
    writer = run([attr_ip("", region="north"), ip_message("b")], pattern="{@region}", missing="error")
    assert texts(writer) == ["north"]


def test_a_missing_placeholder_can_be_kept_verbatim() -> None:
    writer = run([ip_message("a")], pattern="{@nope}", missing="keep")
    assert texts(writer) == ["{@nope}"]


def test_a_pattern_with_an_unusable_placeholder_stops_the_component() -> None:
    """Better than emitting a stream where every IP silently renders a typo."""
    writer = run([ip_message("a")], pattern="{region}")
    assert writer.values == []


def test_number_formatting_is_configurable() -> None:
    ip = fbp_capnp.IP.new_message(content="")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "v"
    kvs[0].value = common_capnp.Value.new_message(f64=3.14159)
    kvs[0].valueType = VALUE_TYPE
    writer = run([PortMessage(PortValue(ip))], pattern="{@v}", number_format=".2f")
    assert texts(writer) == ["3.14"]


def test_the_output_is_tagged_as_text() -> None:
    writer = run([ip_message("a")], pattern="x")
    assert writer.values[0].sysAttributes.contentType == "Text"


def test_bracket_ips_pass_through_unchanged() -> None:
    writer = run([open_bracket_message(), ip_message("a"), close_bracket_message()], pattern="x")
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_a_skipped_ip_still_advances_the_count() -> None:
    """So {count} keeps matching the input position rather than the output position."""
    writer = run(
        [ip_message("a"), attr_ip("", region="north")],
        pattern="{@region}-{count}",
        missing="error",
    )
    assert texts(writer) == ["north-1"]
