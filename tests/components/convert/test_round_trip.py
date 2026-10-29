"""JSON -> Cap'n Proto -> JSON over a real schema.

The point of WP2: a flow can drop into JSON for a few steps and come back to typed messages. If the
two components do not compose, the bridge is not one.
"""

from __future__ import annotations

import json

from mas.schema.common import common_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import STRUCTURED_TEXT_TYPE, VALUE_TYPE
from zalfmas_fbp.components.convert.capnp_to_json import METADATA as TO_JSON_META
from zalfmas_fbp.components.convert.capnp_to_json import CapnpToJson
from zalfmas_fbp.components.convert.json_to_capnp import METADATA as TO_CAPNP_META
from zalfmas_fbp.components.convert.json_to_capnp import JsonToCapnp


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def to_capnp(json_text: str, content_type: str):
    writer = run_process_component(
        JsonToCapnp(TO_CAPNP_META),
        inputs={"in": [ip_message(json_text), done_message()], "conf": conf(content_type=content_type)},
    ).output()
    return writer.values[0]


def to_json(ip, **settings):
    writer = run_process_component(
        CapnpToJson(TO_JSON_META),
        inputs={"in": [PortMessage(PortValue(ip)), done_message()], **({"conf": conf(**settings)} if settings else {})},
    ).output()
    return writer.values[0].content.as_text()


def test_structured_text_round_trips() -> None:
    original = {"type": "toml", "value": "a = 1"}
    built = to_capnp(json.dumps(original), STRUCTURED_TEXT_TYPE)

    assert built.sysAttributes.contentType == STRUCTURED_TEXT_TYPE
    assert json.loads(to_json(built)) == original


def test_a_common_value_round_trips() -> None:
    original = {"a": 1, "b": "x", "c": [1, 2, 3]}
    built = to_capnp(json.dumps(original), VALUE_TYPE)

    assert json.loads(to_json(built)) == original


def test_a_nested_struct_with_a_list_round_trips() -> None:
    """fbp.capnp:IP itself: a struct containing a list of structs and an enum."""
    ip_type = "@0xaf0a1dc4709a5ccf = fbp/fbp.capnp:IP"
    original = {"type": "standard", "attributes": [{"key": "a", "desc": "first"}]}

    built = to_capnp(json.dumps(original), ip_type)
    returned = json.loads(to_json(built))

    assert returned["type"] == "standard"
    assert returned["attributes"][0]["key"] == "a"
    assert returned["attributes"][0]["desc"] == "first"


def test_the_returned_json_can_be_built_again() -> None:
    """Two full laps, so the conversion is stable rather than merely reversible once."""
    original = {"type": "json", "value": '{"nested": true}'}

    first = to_capnp(json.dumps(original), STRUCTURED_TEXT_TYPE)
    middle = to_json(first)
    second = to_capnp(middle, STRUCTURED_TEXT_TYPE)

    assert json.loads(to_json(second)) == original
    assert second.sysAttributes.contentType == first.sysAttributes.contentType
