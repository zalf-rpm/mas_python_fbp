"""The Process template has to work, since it is what a new component is copied from.

It is also where the error-handling rule this library settled on is demonstrated (plan LP5): guard
only the step that can fail on caller data, and let a fault in the component itself stop the
process. If the template got that wrong, every component copied from it would too.
"""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    ip_message_with_attrs,
    open_bracket_message,
    run_process_component,
    text_outputs,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.component_templates.process_component_template import (
    METADATA,
    TemplateProcessComponent,
)


def run(messages, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(TemplateProcessComponent(METADATA), inputs=inputs, outputs=("out",)).output()


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def test_the_template_is_still_a_process_component() -> None:
    assert METADATA.type == "process"


def test_it_forwards_text() -> None:
    assert text_outputs(run([ip_message("hello")])) == ["hello"]


def test_the_prefix_is_applied() -> None:
    assert text_outputs(run([ip_message("world")], prefix="hi ")) == ["hi world"]


def test_it_attaches_the_example_attribute_as_a_typed_value() -> None:
    """Written as a common.Value with its valueType set, which is what makes it readable (D4)."""

    writer = run([ip_message("x")])
    kv = writer.values[0].attributes[0]
    assert kv.key == "processedBy"
    assert kv.valueType == VALUE_TYPE
    assert python_from_attr(kv) == METADATA.info.name


def test_the_example_attribute_can_be_turned_off() -> None:
    writer = run([ip_message("x")], attribute_name=None)
    assert list(writer.values[0].attributes) == []


def test_incoming_attributes_are_preserved() -> None:
    writer = run([ip_message_with_attrs("x", kept="yes")])
    assert "kept" in [kv.key for kv in writer.values[0].attributes]


def test_bracket_ips_are_forwarded_unchanged() -> None:
    writer = run([open_bracket_message(), ip_message("x"), close_bracket_message()])
    assert [str(v.type) for v in writer.values] == ["openBracket", "standard", "closeBracket"]


def test_an_unreadable_ip_is_skipped_by_default() -> None:
    """Bad input from upstream is the component's business to tolerate."""

    unreadable = ip_message(common_capnp.Value.new_message(f64=1.0))
    writer = run([unreadable, ip_message("good")])
    assert text_outputs(writer) == ["good"]


def test_an_unreadable_ip_can_stop_the_process() -> None:
    unreadable = ip_message(common_capnp.Value.new_message(f64=1.0))
    with pytest.raises(ValueError, match="no text could be read"):
        run([unreadable], on_error="fail")


def test_a_fault_in_the_component_is_not_swallowed(monkeypatch) -> None:
    """The rule the template exists to demonstrate: only the payload read is guarded, so a bug in
    the component stops it instead of logging one traceback per IP forever."""

    def boom(self, in_ip):
        msg = "a fault inside the component"
        raise RuntimeError(msg)

    monkeypatch.setattr(TemplateProcessComponent, "_text_of", boom)
    with pytest.raises(RuntimeError, match="a fault inside the component"):
        run([ip_message("x")])


def test_several_ips_in_a_row() -> None:
    assert text_outputs(run([ip_message("a"), ip_message("b"), ip_message("c")])) == ["a", "b", "c"]
