from __future__ import annotations

import json

from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import STRUCTURED_TEXT_TYPE, VALUE_TYPE
from zalfmas_fbp.components.convert.json_to_capnp import METADATA, JsonToCapnp


def run(messages, outputs=("out",), type_port=None, **settings):
    component = JsonToCapnp(METADATA)
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    if type_port is not None:
        ports["type"] = [ip_message(type_port), done_message()]
    return run_process_component(component, inputs=ports, outputs=outputs)


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def test_builds_a_struct_of_the_configured_type() -> None:
    writer = run([ip_message(json.dumps({"type": "toml", "value": "a = 1"}))], content_type=STRUCTURED_TEXT_TYPE)
    built = standard(writer.output())[0].content.as_struct(common_capnp.StructuredText)
    assert (str(built.type), built.value) == ("toml", "a = 1")


def test_the_output_is_tagged_with_the_type_that_was_built() -> None:
    writer = run([ip_message(json.dumps({"value": "x"}))], content_type=STRUCTURED_TEXT_TYPE)
    assert standard(writer.output())[0].sysAttributes.contentType == STRUCTURED_TEXT_TYPE


def test_the_target_type_can_arrive_on_the_type_port() -> None:
    """So the type can come from the flow rather than the component's config."""
    writer = run([ip_message(json.dumps({"value": "x"}))], type_port=STRUCTURED_TEXT_TYPE)
    built = standard(writer.output())[0].content.as_struct(common_capnp.StructuredText)
    assert built.value == "x"


def test_without_any_type_the_component_finishes_quietly() -> None:
    assert run([ip_message("{}")]).output().values == []


def test_an_unresolvable_type_finishes_quietly() -> None:
    assert run([ip_message("{}")], content_type="not a type").output().values == []


def test_a_common_value_target_picks_a_fitting_union_field() -> None:
    writer = run([ip_message(json.dumps({"a": 1}))], content_type=VALUE_TYPE)
    built = standard(writer.output())[0].content.as_struct(common_capnp.Value)
    assert built.which() == "lpair"


def test_unknown_fields_are_an_error_by_default() -> None:
    writer = run([ip_message(json.dumps({"nope": 1}))], content_type=STRUCTURED_TEXT_TYPE)
    assert standard(writer.output()) == []


def test_unknown_fields_can_be_ignored() -> None:
    writer = run(
        [ip_message(json.dumps({"nope": 1, "value": "kept"}))],
        content_type=STRUCTURED_TEXT_TYPE,
        unknown_fields="ignore",
    )
    built = standard(writer.output())[0].content.as_struct(common_capnp.StructuredText)
    assert built.value == "kept"


def test_numbers_are_coerced_to_the_field_type() -> None:
    writer = run([ip_message(json.dumps({"value": 5}))], content_type=STRUCTURED_TEXT_TYPE)
    built = standard(writer.output())[0].content.as_struct(common_capnp.StructuredText)
    assert built.value == "5"


def test_coercion_can_be_turned_off() -> None:
    writer = run(
        [ip_message(json.dumps({"value": 5}))],
        content_type=STRUCTURED_TEXT_TYPE,
        coerce_numbers=False,
    )
    assert standard(writer.output()) == []


def test_malformed_json_is_skipped() -> None:
    writer = run(
        [ip_message("{not json"), ip_message(json.dumps({"value": "ok"}))],
        content_type=STRUCTURED_TEXT_TYPE,
    )
    assert len(standard(writer.output())) == 1


def test_failures_go_to_err_when_it_is_connected() -> None:
    result = run([ip_message("{not json")], outputs=("out", "err"), content_type=STRUCTURED_TEXT_TYPE)
    assert standard(result.output("out")) == []
    assert result.output("err").values[0].content.as_text() == "{not json"


def test_a_traversal_path_selects_part_of_the_input() -> None:
    payload = json.dumps({"wrapper": {"value": "inner"}})
    writer = run([ip_message(payload)], content_type=STRUCTURED_TEXT_TYPE, traversal_path="wrapper")
    built = standard(writer.output())[0].content.as_struct(common_capnp.StructuredText)
    assert built.value == "inner"


def test_attributes_are_preserved() -> None:
    from mas.schema.fbp import fbp_capnp

    from tests.component_harness import PortMessage, PortValue

    ip = fbp_capnp.IP.new_message(content=json.dumps({"value": "x"}))
    kvs = ip.init("attributes", 1)
    kvs[0].key = "region"
    kvs[0].value = common_capnp.Value.new_message(t="north")
    kvs[0].valueType = VALUE_TYPE

    writer = run([PortMessage(PortValue(ip))], content_type=STRUCTURED_TEXT_TYPE)
    assert [kv.key for kv in standard(writer.output())[0].attributes] == ["region"]


def test_bracket_ips_pass_through_unchanged() -> None:
    writer = run(
        [open_bracket_message(), ip_message(json.dumps({"value": "x"})), close_bracket_message()],
        content_type=STRUCTURED_TEXT_TYPE,
    )
    assert [str(v.type) for v in writer.output().values] == ["openBracket", "standard", "closeBracket"]
