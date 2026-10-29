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
from zalfmas_fbp.components.common.values import VALUE_TYPE
from zalfmas_fbp.components.ip.deduplicate_ips import METADATA as DEDUPE_META
from zalfmas_fbp.components.ip.deduplicate_ips import DeduplicateIPs
from zalfmas_fbp.components.ip.gate import METADATA as GATE_META
from zalfmas_fbp.components.ip.gate import Gate
from zalfmas_fbp.components.ip.take_drop_ips import METADATA as TAKE_META
from zalfmas_fbp.components.ip.take_drop_ips import TakeDropIPs


def attr_ip(content, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    kvs = ip.init("attributes", len(attrs))
    for i, (key, value) in enumerate(attrs.items()):
        kvs[i].key = key
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def texts(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def shapes(writer):
    return [str(v.type) for v in writer.values]


# --- take_drop_ips --------------------------------------------------------------------------


def run_take(messages, outputs=("out", "rej"), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(TakeDropIPs(TAKE_META), inputs=ports, outputs=outputs)


NUMBERED = [ip_message(str(i)) for i in range(5)]


def test_first_n() -> None:
    result = run_take(NUMBERED, mode="first_n", n=2)
    assert texts(result.output("out")) == ["0", "1"]
    assert texts(result.output("rej")) == ["2", "3", "4"]


def test_skip_n() -> None:
    assert texts(run_take(NUMBERED, mode="skip_n", n=3).output("out")) == ["3", "4"]


def test_every_nth() -> None:
    assert texts(run_take(NUMBERED, mode="every_nth", n=2).output("out")) == ["0", "2", "4"]


def test_last_n() -> None:
    result = run_take(NUMBERED, mode="last_n", n=2)
    assert texts(result.output("out")) == ["3", "4"]
    assert texts(result.output("rej")) == ["0", "1", "2"]


def test_while_true_stops_at_the_first_failure_and_stays_stopped() -> None:
    messages = [attr_ip("a", ok="y"), attr_ip("b", ok="n"), attr_ip("c", ok="y")]
    result = run_take(messages, mode="while_true", predicate={"left": "@ok", "op": "eq", "right": "y"})
    assert texts(result.output("out")) == ["a"]


def test_until_true_stops_at_the_first_success() -> None:
    messages = [attr_ip("a", ok="n"), attr_ip("b", ok="y"), attr_ip("c", ok="n")]
    result = run_take(messages, mode="until_true", predicate={"left": "@ok", "op": "eq", "right": "y"})
    assert texts(result.output("out")) == ["a"]


def test_a_predicate_mode_without_a_predicate_stops_the_component() -> None:
    assert run_take(NUMBERED, mode="while_true").output("out").values == []


def test_the_count_can_restart_per_substream() -> None:
    messages = [
        open_bracket_message(),
        ip_message("a1"),
        ip_message("a2"),
        close_bracket_message(),
        open_bracket_message(),
        ip_message("b1"),
        ip_message("b2"),
        close_bracket_message(),
    ]
    result = run_take(messages, outputs=("out",), mode="first_n", n=1, scope="substream")
    assert texts(result.output("out")) == ["a1", "b1"]


def test_the_count_spans_the_stream_by_default() -> None:
    messages = [
        open_bracket_message(),
        ip_message("a1"),
        close_bracket_message(),
        open_bracket_message(),
        ip_message("b1"),
        close_bracket_message(),
    ]
    result = run_take(messages, outputs=("out",), mode="first_n", n=1)
    assert texts(result.output("out")) == ["a1"]


def test_last_n_per_substream() -> None:
    messages = [
        open_bracket_message(),
        ip_message("a1"),
        ip_message("a2"),
        close_bracket_message(),
        open_bracket_message(),
        ip_message("b1"),
        ip_message("b2"),
        close_bracket_message(),
    ]
    result = run_take(messages, outputs=("out",), mode="last_n", n=1, scope="substream")
    assert texts(result.output("out")) == ["a2", "b2"]


def test_brackets_pass_through() -> None:
    messages = [open_bracket_message(), ip_message("a"), close_bracket_message()]
    result = run_take(messages, outputs=("out",), mode="first_n", n=1)
    assert shapes(result.output("out")) == ["openBracket", "standard", "closeBracket"]


# --- deduplicate_ips ------------------------------------------------------------------------


def run_dedupe(messages, outputs=("out", "dup"), **settings):
    ports: dict = {"in": [*messages, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(DeduplicateIPs(DEDUPE_META), inputs=ports, outputs=outputs)


def test_deduplicates_by_a_selector() -> None:
    messages = [attr_ip("a", id="1"), attr_ip("b", id="1"), attr_ip("c", id="2")]
    result = run_dedupe(messages, selector="@id")
    assert texts(result.output("out")) == ["a", "c"]
    assert texts(result.output("dup")) == ["b"]


def test_deduplicates_whole_contents_without_a_selector() -> None:
    result = run_dedupe([ip_message("same"), ip_message("same"), ip_message("other")])
    assert texts(result.output("out")) == ["same", "other"]


def test_the_memory_can_reset_per_substream() -> None:
    messages = [
        open_bracket_message(),
        attr_ip("a", id="1"),
        close_bracket_message(),
        open_bracket_message(),
        attr_ip("b", id="1"),
        close_bracket_message(),
    ]
    result = run_dedupe(messages, outputs=("out",), selector="@id", scope="substream")
    assert texts(result.output("out")) == ["a", "b"]


def test_the_memory_spans_the_stream_by_default() -> None:
    messages = [
        open_bracket_message(),
        attr_ip("a", id="1"),
        close_bracket_message(),
        open_bracket_message(),
        attr_ip("b", id="1"),
        close_bracket_message(),
    ]
    result = run_dedupe(messages, outputs=("out",), selector="@id")
    assert texts(result.output("out")) == ["a"]


def test_a_window_forgets_older_keys() -> None:
    messages = [attr_ip(c, id=c) for c in "abca"]
    result = run_dedupe(messages, outputs=("out",), selector="@id", window=2)
    assert texts(result.output("out")) == ["a", "b", "c", "a"]


def test_an_unresolvable_key_passes_through_rather_than_collapsing() -> None:
    """Otherwise every IP without the key would look like a duplicate of the first."""
    result = run_dedupe([ip_message("a"), ip_message("b")], outputs=("out",), selector="@missing")
    assert texts(result.output("out")) == ["a", "b"]


# --- gate ------------------------------------------------------------------------------------


def run_gate(messages, signals, **settings):
    ports: dict = {"in": [*messages, done_message()], "signal": [*signals, done_message()]}
    if settings:
        ports["conf"] = conf(**settings)
    return run_process_component(Gate(GATE_META), inputs=ports, outputs=("out",))


def test_each_signal_releases_one_held_ip() -> None:
    result = run_gate([ip_message("a"), ip_message("b"), ip_message("c")], [ip_message("go")])
    assert len(texts(result.output("out"))) == 1


def test_a_signal_can_release_several() -> None:
    result = run_gate([ip_message(str(i)) for i in range(4)], [ip_message("go")], n=2)
    assert len(texts(result.output("out"))) == 2


def test_open_close_lets_everything_through_while_open() -> None:
    result = run_gate(
        [ip_message(str(i)) for i in range(3)],
        [ip_message("open")],
        mode="open_close",
    )
    assert len(texts(result.output("out"))) == 3


def test_starting_open_needs_no_signal() -> None:
    result = run_gate([ip_message("a")], [], mode="open_close", start_open=True)
    assert texts(result.output("out")) == ["a"]


def test_a_signal_arriving_before_its_ip_still_releases_it() -> None:
    """Signals are credits: with two inputs there is no guaranteed arrival order, so a signal that
    finds nothing held must still release the IP that arrives next.
    """
    component = Gate(GATE_META)
    result = run_process_component(
        component,
        inputs={"signal": [ip_message("go"), done_message()], "in": [ip_message("a"), done_message()]},
        outputs=("out",),
    )
    assert texts(result.output("out")) == ["a"]


def test_the_gate_stops_once_the_input_is_done_and_nothing_is_held() -> None:
    """Rather than draining signals it can no longer use."""
    component = Gate(GATE_META)
    result = run_process_component(
        component,
        inputs={"signal": [ip_message("go")] * 3 + [done_message()], "in": [done_message()]},
        outputs=("out",),
    )
    assert texts(result.output("out")) == []
    assert not component._held


def test_ips_left_held_when_the_signals_stop_are_not_emitted() -> None:
    result = run_gate([ip_message("a"), ip_message("b")], [ip_message("go")])
    assert len(texts(result.output("out"))) == 1


def test_drop_while_closed_discards_rather_than_holding() -> None:
    component = Gate(GATE_META)
    run_process_component(
        component,
        inputs={
            "in": [ip_message("a"), done_message()],
            "signal": [done_message()],
            "conf": conf(mode="drop_while_closed"),
        },
        outputs=("out",),
    )
    assert component._dropped == 1
    assert not component._held


def test_max_held_drops_the_oldest() -> None:
    component = Gate(GATE_META)
    run_process_component(
        component,
        inputs={
            "in": [ip_message(str(i)) for i in range(5)] + [done_message()],
            "signal": [done_message()],
            "conf": conf(max_held=2),
        },
        outputs=("out",),
    )
    assert component._dropped == 3
    assert len(component._held) == 2
