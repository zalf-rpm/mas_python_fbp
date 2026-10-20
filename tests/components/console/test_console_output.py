"""console_output, converted from Runnable to Process style (plan LP1).

The behaviour the old version had - print each IP's text content to stdout, one line each - is the
default here, and the first test pins exactly that.
"""

from __future__ import annotations

import json
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
    run_process_component,
)
from zalfmas_fbp.components.common.values import STRUCTURED_TEXT_TYPE, VALUE_TYPE
from zalfmas_fbp.components.console.console_output import METADATA, ConsoleOutput


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def run(messages, **settings):
    component = ConsoleOutput(METADATA)
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = conf(**settings)
    run_process_component(component, inputs=inputs, outputs=())
    return component


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"
    assert [p.name for p in METADATA.outPorts] == ["log"]  # a sink, plus the runtime-owned log port


def test_text_content_goes_to_stdout_one_line_each(capsys: Any) -> None:
    """What the Runnable version did, unchanged."""
    run([ip_message("hello console"), ip_message("second")])
    assert capsys.readouterr().out == "hello console\nsecond\n"


def test_brackets_are_not_printed_by_default(capsys: Any) -> None:
    run([open_bracket_message(), ip_message("a"), close_bracket_message()])
    assert capsys.readouterr().out == "a\n"


def test_brackets_can_be_shown_to_reveal_the_substream_shape(capsys: Any) -> None:
    run(
        [open_bracket_message(), ip_message("a"), close_bracket_message()],
        show=["type", "content"],
        show_brackets=True,
    )
    assert capsys.readouterr().out == "openBracket\nstandard a\ncloseBracket\n"


def test_a_running_count_can_be_shown(capsys: Any) -> None:
    run([ip_message("a"), ip_message("b")], show=["count", "content"])
    assert capsys.readouterr().out == "#1 a\n#2 b\n"


def test_attributes_can_be_shown(capsys: Any) -> None:
    ip = fbp_capnp.IP.new_message(content="x")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "region"
    kvs[0].value = common_capnp.Value.new_message(t="north")
    kvs[0].valueType = VALUE_TYPE

    run([PortMessage(PortValue(ip))], show=["attributes"])
    assert json.loads(capsys.readouterr().out) == {"region": "north"}


def test_typed_content_is_rendered_through_its_content_type(capsys: Any) -> None:
    ip = fbp_capnp.IP.new_message(
        content=common_capnp.StructuredText.new_message(type="json", value='{"a": 1}'),
        sysAttributes={"contentType": STRUCTURED_TEXT_TYPE},
    )
    run([PortMessage(PortValue(ip))])
    assert capsys.readouterr().out.strip() == "{'a': 1}"


def test_content_can_be_rendered_as_json(capsys: Any) -> None:
    ip = fbp_capnp.IP.new_message(
        content=common_capnp.StructuredText.new_message(type="json", value='{"a": 1}'),
        sysAttributes={"contentType": STRUCTURED_TEXT_TYPE},
    )
    run([PortMessage(PortValue(ip))], as_json=True)
    assert json.loads(capsys.readouterr().out) == {"a": 1}


def test_untyped_struct_content_is_reported_as_unreadable_not_guessed(capsys: Any) -> None:
    """D14: casting to a guessed schema would silently misread it."""
    ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(i64=42))
    run([PortMessage(PortValue(ip))])
    assert "unreadable content" in capsys.readouterr().out


def test_long_lines_can_be_truncated(capsys: Any) -> None:
    run([ip_message("x" * 100)], max_chars=10)
    assert "+90 chars" in capsys.readouterr().out


def test_it_counts_what_it_printed() -> None:
    component = run([ip_message("a"), ip_message("b")])
    assert component.printed == 2


def test_an_empty_stream_prints_nothing(capsys: Any) -> None:
    run([])
    assert capsys.readouterr().out == ""
