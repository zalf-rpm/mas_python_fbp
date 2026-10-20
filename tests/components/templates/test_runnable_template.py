"""The Runnable template has to work, since it is what a Runnable component is copied from.

Runnable style is still supported: it is what the C++ implementation uses, and Python is so far the
only one implementing the Process interface. Process style is the recommendation, but a broken
template for the other style is still a broken template.
"""

from __future__ import annotations

from typing import Any

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_standard_component,
    text_outputs,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.component_templates.runnable_component_template import (
    METADATA,
    run_component,
)


def run(messages, monkeypatch, **config):
    settings = {"name": "template", "prefix": "", "attribute_name": "processedBy", **config}
    return run_standard_component(
        run_component,
        monkeypatch,
        inputs={"in": [*messages, done_message()], "conf": [done_message()]},
        outputs=("out",),
        config=settings,
    ).output()


def shapes(writer):
    return [str(v.type) for v in writer.values]


def test_the_template_is_still_a_standard_component() -> None:
    assert METADATA.type == "standard"


def test_it_forwards_and_prefixes_text(monkeypatch: Any) -> None:
    writer = run([ip_message("hello")], monkeypatch, prefix=">> ")
    assert text_outputs(writer) == [">> hello"]


def test_it_runs_with_the_metadata_defaults(monkeypatch: Any) -> None:
    """A template nobody has configured yet must still work."""
    defaults = METADATA.default_config_values()
    writer = run([ip_message("hello")], monkeypatch, **defaults)
    assert text_outputs(writer) == ["hello"]


def test_it_attaches_the_example_attribute_as_a_typed_value(monkeypatch: Any) -> None:
    writer = run([ip_message("hello")], monkeypatch)
    attrs = {kv.key: python_from_attr(kv) for kv in writer.values[0].attributes}
    assert attrs == {"processedBy": "template"}


def test_the_example_attribute_can_be_turned_off(monkeypatch: Any) -> None:
    writer = run([ip_message("hello")], monkeypatch, attribute_name="")
    assert list(writer.values[0].attributes) == []


def test_incoming_attributes_are_preserved(monkeypatch: Any) -> None:
    ip = fbp_capnp.IP.new_message(content="hello")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "region"
    kvs[0].value = common_capnp.Value.new_message(t="north")
    kvs[0].valueType = VALUE_TYPE

    writer = run([PortMessage(PortValue(ip))], monkeypatch)
    attrs = {kv.key: python_from_attr(kv) for kv in writer.values[0].attributes}
    assert attrs == {"region": "north", "processedBy": "template"}


def test_bracket_ips_are_forwarded_unchanged(monkeypatch: Any) -> None:
    """It rebuilt every IP with new_message(content=...), which drops the type - so a bracket came
    out as a standard IP carrying the prefix, destroying the substream.
    """
    writer = run(
        [open_bracket_message(), ip_message("a"), close_bracket_message()],
        monkeypatch,
        prefix=">> ",
    )
    assert shapes(writer) == ["openBracket", "standard", "closeBracket"]
    assert text_outputs(writer)[1] == ">> a"


def test_the_example_attribute_is_written_as_a_typed_value(monkeypatch: Any) -> None:
    """D4: the library reads attributes as common.Value with valueType, and a template should
    demonstrate the convention rather than leaving a raw string.
    """
    writer = run([ip_message("hello")], monkeypatch)
    attr = writer.values[0].attributes[0]
    assert attr._has("valueType")
    assert attr.value.as_struct(common_capnp.Value).t == "template"
