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
from zalfmas_fbp.components.json.split_json import METADATA, SplitJson


def attr_ip(content, **attrs):
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
    return run_process_component(SplitJson(METADATA), inputs=ports, outputs=("out",)).output()


def items(writer):
    return [json.loads(v.content.as_text()) for v in writer.values if str(v.type) == "standard"]


def shapes(writer):
    return [str(v.type) for v in writer.values]


def test_splits_a_list_into_one_ip_per_item() -> None:
    writer = run([ip_message(json.dumps([1, 2, 3]))])
    assert items(writer) == [1, 2, 3]
    assert shapes(writer) == ["openBracket", "standard", "standard", "standard", "closeBracket"]


def test_the_substream_wrapper_is_optional() -> None:
    writer = run([ip_message(json.dumps([1, 2]))], wrap_in_substream=False)
    assert shapes(writer) == ["standard", "standard"]


def test_object_values_mode() -> None:
    writer = run([ip_message(json.dumps({"a": 1, "b": 2}))], mode="object_values")
    assert items(writer) == [1, 2]


def test_object_entries_mode_keeps_the_keys_in_the_content() -> None:
    writer = run([ip_message(json.dumps({"a": 1}))], mode="object_entries")
    assert items(writer) == [{"key": "a", "value": 1}]


def test_keys_can_be_attached_as_an_attribute() -> None:
    writer = run([ip_message(json.dumps({"a": 1, "b": 2}))], mode="object_values", key_attr="k")
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert [python_from_attr(v.attributes[0]) for v in standard] == ["a", "b"]


def test_indices_can_be_attached_as_an_attribute() -> None:
    writer = run([ip_message(json.dumps(["x", "y"]))], index_attr="i")
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert [python_from_attr(v.attributes[0]) for v in standard] == [0, 1]


def test_the_item_count_lands_on_the_close_bracket() -> None:
    """Mirrors what 'Split bracketed stream' writes, so downstream can rely on one convention."""
    writer = run([ip_message(json.dumps([1, 2, 3]))])
    close = writer.values[-1]
    assert str(close.type) == "closeBracket"
    assert close.attributes[0].key == "substream_length"
    assert python_from_attr(close.attributes[0]) == 3


def test_a_traversal_path_selects_the_list_to_split() -> None:
    payload = {"meta": {"run": 1}, "rows": [10, 20]}
    assert items(run([ip_message(json.dumps(payload))], traversal_path="rows")) == [10, 20]


def test_parent_fields_can_be_copied_onto_every_item() -> None:
    """The 'keep the header fields with each row' case."""
    payload = {"run": "r1", "rows": [1, 2]}
    writer = run(
        [ip_message(json.dumps(payload))],
        traversal_path="rows",
        copy_parent_paths={"run": "run"},
    )
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert all(python_from_attr(v.attributes[0]) == "r1" for v in standard)
    assert len(standard) == 2


def test_an_unresolvable_parent_path_is_skipped_not_fatal() -> None:
    payload = {"rows": [1]}
    writer = run([ip_message(json.dumps(payload))], traversal_path="rows", copy_parent_paths={"x": "nope"})
    assert items(writer) == [1]


def test_incoming_attributes_are_carried_onto_each_item() -> None:
    writer = run([attr_ip(json.dumps([1, 2]), region="north")])
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert all(python_from_attr(v.attributes[0]) == "north" for v in standard)


def test_an_atomic_document_is_forwarded_unchanged() -> None:
    writer = run([ip_message(json.dumps(42))])
    assert shapes(writer) == ["standard"]
    assert items(writer) == [42]


def test_an_object_in_list_mode_is_forwarded_unchanged() -> None:
    writer = run([ip_message(json.dumps({"a": 1}))])
    assert shapes(writer) == ["standard"]


def test_malformed_json_is_skipped() -> None:
    writer = run([ip_message("{not json"), ip_message(json.dumps([1]))])
    assert items(writer) == [1]


def test_malformed_json_can_be_passed_through() -> None:
    writer = run([ip_message("{not json")], on_error="pass_through")
    assert [v.content.as_text() for v in writer.values] == ["{not json"]


def test_an_empty_list_still_produces_an_empty_substream() -> None:
    writer = run([ip_message(json.dumps([]))])
    assert shapes(writer) == ["openBracket", "closeBracket"]


def test_incoming_bracket_ips_pass_through() -> None:
    writer = run([open_bracket_message(), ip_message(json.dumps([1])), close_bracket_message()])
    assert shapes(writer) == ["openBracket", "openBracket", "standard", "closeBracket", "closeBracket"]


def test_the_output_is_tagged_as_json() -> None:
    writer = run([ip_message(json.dumps([1]))])
    standard = [v for v in writer.values if str(v.type) == "standard"]
    assert standard[0].sysAttributes.contentType == "Text (JSON)"
