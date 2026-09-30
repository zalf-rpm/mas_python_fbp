from __future__ import annotations

import json
import logging

from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import python_from_attr, python_from_value
from zalfmas_fbp.components.ip.discard import METADATA as DISCARD_META
from zalfmas_fbp.components.ip.discard import Discard
from zalfmas_fbp.components.ip.on_stream_end import METADATA as END_META
from zalfmas_fbp.components.ip.on_stream_end import OnStreamEnd


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def run_end(messages, outputs=("out", "end"), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(OnStreamEnd(END_META), inputs=ports, outputs=outputs)


# --- on_stream_end ------------------------------------------------------------------------------


def test_emits_one_signal_when_the_stream_closes() -> None:
    result = run_end([ip_message("a"), ip_message("b")])
    assert [v.content.as_text() for v in result.output("out").values] == ["a", "b"]
    assert len(result.output("end").values) == 1
    assert result.output("end").values[0].content.as_text() == "done"


def test_the_signal_carries_how_many_ips_went_past() -> None:
    result = run_end([ip_message("a"), ip_message("b"), ip_message("c")])
    assert python_from_attr(result.output("end").values[0].attributes[0]) == 3


def test_brackets_are_not_counted_as_ips() -> None:
    result = run_end([open_bracket_message(), ip_message("a"), close_bracket_message()])
    assert python_from_attr(result.output("end").values[0].attributes[0]) == 1


def test_a_signal_can_be_emitted_per_substream() -> None:
    result = run_end(
        [
            open_bracket_message(),
            ip_message("a"),
            ip_message("b"),
            close_bracket_message(),
            open_bracket_message(),
            ip_message("c"),
            close_bracket_message(),
        ],
        trigger_on="substream_end",
    )
    counts = [python_from_attr(v.attributes[0]) for v in result.output("end").values]
    assert counts == [2, 1]


def test_both_emits_per_substream_and_at_the_end() -> None:
    result = run_end(
        [open_bracket_message(), ip_message("a"), close_bracket_message()],
        trigger_on="both",
    )
    assert len(result.output("end").values) == 2


def test_the_signal_content_is_configurable() -> None:
    result = run_end([ip_message("a")], content="finished")
    assert result.output("end").values[0].content.as_text() == "finished"


def test_the_signal_can_be_a_common_value() -> None:
    result = run_end([ip_message("a")], as_type="value")
    signal = result.output("end").values[0]
    assert python_from_value(signal.content.as_struct(common_capnp.Value)) == "done"


def test_forwarding_can_be_turned_off_to_make_it_a_sink() -> None:
    result = run_end([ip_message("a")], forward_input=False)
    assert result.output("out").values == []
    assert len(result.output("end").values) == 1


def test_a_signal_is_emitted_even_for_an_empty_stream() -> None:
    """A flow sequenced after this must not stall just because nothing came through."""
    result = run_end([])
    assert len(result.output("end").values) == 1
    assert python_from_attr(result.output("end").values[0].attributes[0]) == 0


# --- discard ------------------------------------------------------------------------------------


def test_discard_drains_the_stream() -> None:
    component = Discard(DISCARD_META)
    run_process_component(
        component,
        inputs={"in": [ip_message("a"), ip_message("b"), done_message()]},
        outputs=(),
    )
    assert component.discarded == 2


def test_discard_counts_brackets_separately() -> None:
    component = Discard(DISCARD_META)
    run_process_component(
        component,
        inputs={"in": [open_bracket_message(), ip_message("a"), close_bracket_message(), done_message()]},
        outputs=(),
    )
    assert (component.discarded, component.brackets_discarded) == (1, 2)


def test_discard_reports_progress_when_asked(caplog) -> None:
    component = Discard(DISCARD_META)
    with caplog.at_level(logging.INFO):
        run_process_component(
            component,
            inputs={
                "in": [ip_message(str(i)) for i in range(4)] + [done_message()],
                "conf": conf(report_every=2),
            },
            outputs=(),
        )
    progress = [m.getMessage() for m in caplog.records if "so far" in m.getMessage()]
    assert len(progress) == 2
