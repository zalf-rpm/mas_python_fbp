"""Substream handling in add_attribute, add_content and lift_attributes (plan LP4).

All three mishandled brackets before this: add_attribute and add_content consumed a partner IP from
their second input for every bracket, so a substream desynchronised the pairing, and
lift_attributes rebuilt the IP without its type, turning a bracket into a standard IP and
destroying the substream.
"""

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
from zalfmas_fbp.components.ip.add_attribute import METADATA as ADD_ATTR_META
from zalfmas_fbp.components.ip.add_attribute import Component as AddAttribute
from zalfmas_fbp.components.ip.add_content import METADATA as ADD_CONTENT_META
from zalfmas_fbp.components.ip.add_content import Component as AddContent
from zalfmas_fbp.components.ip.lift_attributes import METADATA as LIFT_META
from zalfmas_fbp.components.ip.lift_attributes import Component as LiftAttributes


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def shapes(writer):
    return [str(v.type) for v in writer.values]


def contents(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


SUBSTREAM = [open_bracket_message(), ip_message("a"), ip_message("b"), close_bracket_message()]


# --- add_attribute ------------------------------------------------------------------------------


def test_add_attribute_forwards_brackets_without_consuming_a_partner() -> None:
    """One 'attr' IP per *standard* IP: a bracket must not eat one, or the pairing slips."""
    result = run_process_component(
        AddAttribute(ADD_ATTR_META),
        inputs={
            "in": [*SUBSTREAM, done_message()],
            "attr": [ip_message("A"), ip_message("B"), done_message()],
        },
        outputs=("out",),
    ).output()

    assert shapes(result) == ["openBracket", "standard", "standard", "closeBracket"]
    assert contents(result) == ["a", "b"]
    standard = [v for v in result.values if str(v.type) == "standard"]
    assert [python_from_attr(v.attributes[0]) for v in standard] == ["A", "B"]


def test_add_attribute_leaves_brackets_without_the_attribute() -> None:
    result = run_process_component(
        AddAttribute(ADD_ATTR_META),
        inputs={
            "in": [*SUBSTREAM, done_message()],
            "attr": [ip_message("A"), ip_message("B"), done_message()],
        },
        outputs=("out",),
    ).output()
    assert list(result.values[0].attributes) == []


# --- add_content --------------------------------------------------------------------------------


def test_add_content_forwards_brackets_without_consuming_a_partner() -> None:
    result = run_process_component(
        AddContent(ADD_CONTENT_META),
        inputs={
            "in": [*SUBSTREAM, done_message()],
            "content": [ip_message("X"), ip_message("Y"), done_message()],
        },
        outputs=("out",),
    ).output()

    assert shapes(result) == ["openBracket", "standard", "standard", "closeBracket"]
    assert contents(result) == ["X", "Y"]


def test_add_content_does_not_overwrite_a_brackets_content() -> None:
    result = run_process_component(
        AddContent(ADD_CONTENT_META),
        inputs={
            "in": [*SUBSTREAM, done_message()],
            "content": [ip_message("X"), ip_message("Y"), done_message()],
        },
        outputs=("out",),
    ).output()
    assert result.values[0].content.as_text() == ""


# --- lift_attributes ----------------------------------------------------------------------------


def test_lift_attributes_keeps_bracket_ips_as_brackets() -> None:
    """It rebuilt the IP without its type, so a bracket came out as a standard IP."""
    ip = fbp_capnp.IP.new_message(content="a")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "src"
    kvs[0].value = common_capnp.Value.new_message(t="v")
    kvs[0].valueType = VALUE_TYPE

    result = run_process_component(
        LiftAttributes(LIFT_META),
        inputs={
            "in": [open_bracket_message(), PortMessage(PortValue(ip)), close_bracket_message(), done_message()],
            "conf": conf(lift_from_attr="src", lifted_attrs=[]),
        },
        outputs=("out",),
    ).output()

    assert shapes(result) == ["openBracket", "standard", "closeBracket"]


def test_lift_attributes_passes_plain_ips_through_with_their_attributes() -> None:
    ip = fbp_capnp.IP.new_message(content="a")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "src"
    kvs[0].value = common_capnp.Value.new_message(t="v")
    kvs[0].valueType = VALUE_TYPE

    result = run_process_component(
        LiftAttributes(LIFT_META),
        inputs={
            "in": [PortMessage(PortValue(ip)), done_message()],
            "conf": conf(lift_from_attr="src", lifted_attrs=[]),
        },
        outputs=("out",),
    ).output()

    assert [kv.key for kv in result.values[0].attributes] == ["src"]
