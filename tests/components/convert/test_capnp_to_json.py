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
from zalfmas_fbp.components.common.values import STRUCTURED_TEXT_TYPE, VALUE_TYPE
from zalfmas_fbp.components.convert.capnp_to_json import METADATA, CapnpToJson

IP_TYPE = "@0xaf0a1dc4709a5ccf = fbp/fbp.capnp:IP"


def typed_ip(content, content_type=None, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    if content_type:
        ip.sysAttributes.contentType = content_type
    if attrs:
        kvs = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            kvs[i].key = key
            kvs[i].value = common_capnp.Value.new_message(t=value)
            kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(messages, outputs=("out",), **settings):
    component = CapnpToJson(METADATA)
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(component, inputs=ports, outputs=outputs)


def emitted(writer):
    return [json.loads(v.content.as_text()) for v in writer.values if str(v.type) == "standard"]


def test_converts_a_struct_using_the_type_the_ip_declares() -> None:
    message = common_capnp.StructuredText.new_message(type="json", value="{}")
    assert emitted(run([typed_ip(message, STRUCTURED_TEXT_TYPE)]).output()) == [{"value": "{}", "type": "json"}]


def test_a_configured_type_covers_ips_that_carry_none() -> None:
    message = common_capnp.StructuredText.new_message(type="toml", value="a = 1")
    writer = run([typed_ip(message)], content_type=STRUCTURED_TEXT_TYPE).output()
    assert emitted(writer) == [{"value": "a = 1", "type": "toml"}]


def test_the_ips_own_type_wins_over_the_configured_one() -> None:
    message = common_capnp.Value.new_message(i64=7)
    writer = run([typed_ip(message, VALUE_TYPE)], content_type=STRUCTURED_TEXT_TYPE).output()
    assert emitted(writer) == [7]


def test_common_value_is_converted_not_left_opaque() -> None:
    """to_dict() leaves Value.lpair's generic fst/snd opaque, so Value needs its own path."""
    message = common_capnp.Value.new_message(
        lpair=[common_capnp.Pair.new_message(fst="a", snd=common_capnp.Value.new_message(i64=1))],
    )
    assert emitted(run([typed_ip(message, VALUE_TYPE)]).output()) == [{"a": 1}]


def test_text_content_needs_no_type_and_becomes_a_json_string() -> None:
    assert emitted(run([ip_message("plain")]).output()) == ["plain"]


def test_json_text_is_encoded_again_by_default() -> None:
    """Faithful: the content really is a string, and treating it as JSON could change its meaning."""
    assert emitted(run([ip_message('{"a": 1}')]).output()) == ['{"a": 1}']


def test_json_text_can_be_parsed_instead_of_re_encoded() -> None:
    """The JSON components here emit JSON as untyped Text, which would otherwise encode twice."""
    assert emitted(run([ip_message('{"a": 1}')], parse_text_as_json=True).output()) == [{"a": 1}]


def test_text_that_only_looks_like_json_survives_parse_text_as_json() -> None:
    assert emitted(run([ip_message("not json {")], parse_text_as_json=True).output()) == ["not json {"]


def test_untyped_struct_content_is_not_guessed() -> None:
    """D14: casting to a guessed schema would silently misread, so it is an error instead."""
    assert run([typed_ip(common_capnp.Value.new_message(i64=1))]).output().values == []


def test_unconvertible_ips_go_to_err_when_it_is_connected() -> None:
    result = run([typed_ip(common_capnp.Value.new_message(i64=1))], outputs=("out", "err"))
    assert result.output("out").values == []
    assert len(result.output("err").values) == 1


def test_pass_through_forwards_the_input_when_there_is_no_err_port() -> None:
    writer = run([typed_ip(common_capnp.Value.new_message(i64=1))], on_error="pass_through").output()
    assert len(writer.values) == 1


def test_the_output_is_tagged_as_json() -> None:
    writer = run([ip_message("x")]).output()
    assert writer.values[0].sysAttributes.contentType == "Text (JSON)"


def test_attributes_are_preserved() -> None:
    writer = run([typed_ip("x", None, region="north")]).output()
    assert [kv.key for kv in writer.values[0].attributes] == ["region"]


def test_attributes_can_be_folded_into_the_json_instead() -> None:
    writer = run([typed_ip("x", None, region="north")], include_attributes=True).output()
    assert emitted(writer) == [{"content": "x", "attributes": {"region": "north"}}]


def test_a_traversal_path_selects_part_of_the_converted_structure() -> None:
    message = common_capnp.StructuredText.new_message(type="json", value="{}")
    writer = run([typed_ip(message, STRUCTURED_TEXT_TYPE)], traversal_path="type").output()
    assert emitted(writer) == ["json"]


def test_an_unresolvable_traversal_path_is_an_error_for_that_ip() -> None:
    message = common_capnp.StructuredText.new_message(type="json", value="{}")
    writer = run([typed_ip(message, STRUCTURED_TEXT_TYPE)], traversal_path="nope").output()
    assert writer.values == []


def test_data_fields_are_encoded_as_configured() -> None:
    message = common_capnp.Value.new_message(d=b"\x01\x02")
    assert emitted(run([typed_ip(message, VALUE_TYPE)], data_as="hex").output()) == ["0102"]


def test_indentation_is_configurable() -> None:
    message = common_capnp.StructuredText.new_message(type="json", value="{}")
    writer = run([typed_ip(message, STRUCTURED_TEXT_TYPE)], indent=2).output()
    assert "\n" in writer.values[0].content.as_text()


def test_bracket_ips_pass_through_unchanged() -> None:
    writer = run([open_bracket_message(), ip_message("a"), close_bracket_message()]).output()
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]
