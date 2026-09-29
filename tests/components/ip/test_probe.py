from __future__ import annotations

import json
import logging

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    ip_message_with_attrs,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE
from zalfmas_fbp.components.ip.probe import METADATA, Probe


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def run(messages, outputs=("out",), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(Probe(METADATA), inputs=ports, outputs=outputs)


def logged(caplog):
    return [record.getMessage() for record in caplog.records if "probe" in record.name]


def test_forwards_every_ip_unchanged(caplog) -> None:
    with caplog.at_level(logging.INFO):
        writer = run([ip_message("a"), ip_message("b")]).output()
    assert [value.content.as_text() for value in writer.values] == ["a", "b"]


def test_reports_content_and_a_running_count(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([ip_message("alpha"), ip_message("beta")], label="tap")
    messages = [m for m in logged(caplog) if m.startswith("tap:")]
    assert "#1" in messages[0]
    assert "alpha" in messages[0]
    assert "#2" in messages[1]


def test_brackets_are_forwarded_and_counted(caplog) -> None:
    with caplog.at_level(logging.INFO):
        writer = run([open_bracket_message(), ip_message("a"), close_bracket_message()]).output()
    assert [str(value.type) for value in writer.values] == ["openBracket", "standard", "closeBracket"]
    assert any("openBracket" in m for m in logged(caplog))


def test_brackets_can_be_excluded_from_reporting_but_are_still_forwarded(caplog) -> None:
    with caplog.at_level(logging.INFO):
        writer = run(
            [open_bracket_message(), ip_message("a"), close_bracket_message()],
            include_brackets=False,
            label="tap",
        ).output()
    assert len(writer.values) == 3
    per_ip = [m for m in logged(caplog) if m.startswith("tap:") and "finished" not in m]
    assert len(per_ip) == 1


def test_every_nth_samples_the_stream(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([ip_message(str(i)) for i in range(6)], every_nth=3, label="tap")
    per_ip = [m for m in logged(caplog) if m.startswith("tap:") and "finished" not in m]
    assert len(per_ip) == 2


def test_first_n_stops_reporting_but_not_forwarding(caplog) -> None:
    with caplog.at_level(logging.INFO):
        writer = run([ip_message(str(i)) for i in range(5)], first_n=2, label="tap").output()
    per_ip = [m for m in logged(caplog) if m.startswith("tap:") and "finished" not in m]
    assert len(per_ip) == 2
    assert len(writer.values) == 5


def test_long_content_is_truncated(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([ip_message("x" * 100)], max_content_chars=10, label="tap")
    assert any("+90 chars" in m for m in logged(caplog))


def test_attributes_can_be_reported(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([ip_message_with_attrs("a", region="north")], show=["attributes"], label="tap")
    assert any("north" in m for m in logged(caplog))


def test_typed_content_is_rendered_via_its_content_type(caplog) -> None:
    ip = fbp_capnp.IP.new_message(
        content=common_capnp.Value.new_message(i64=42),
        sysAttributes={"contentType": VALUE_TYPE},
    )
    with caplog.at_level(logging.INFO):
        run([PortMessage(PortValue(ip))], label="tap")
    assert any("42" in m for m in logged(caplog))


def test_untyped_struct_content_is_reported_as_unreadable_not_guessed(caplog) -> None:
    """D14: casting to a guessed struct schema would silently misread."""
    ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(i64=42))
    with caplog.at_level(logging.INFO):
        run([PortMessage(PortValue(ip))], label="tap")
    assert any("unreadable content" in m for m in logged(caplog))


def test_a_summary_is_logged_when_the_input_closes(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([open_bracket_message(), ip_message("a"), close_bracket_message()], label="tap")
    summary = [m for m in logged(caplog) if "finished, observed" in m]
    assert len(summary) == 1
    assert "'standard': 1" in summary[0]
    assert "'openBracket': 1" in summary[0]


def test_the_summary_can_be_suppressed(caplog) -> None:
    with caplog.at_level(logging.INFO):
        run([ip_message("a")], emit_summary_on_close=False, label="tap")
    assert not [m for m in logged(caplog) if "finished, observed" in m]


def test_acts_as_a_sink_when_out_is_not_connected(caplog) -> None:
    with caplog.at_level(logging.INFO):
        result = run([ip_message("a"), ip_message("b")], outputs=(), label="tap")
    assert [m for m in logged(caplog) if m.startswith("tap:") and "finished" not in m]
    assert result.outputs == {}
