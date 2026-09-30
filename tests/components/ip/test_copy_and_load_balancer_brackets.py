"""How copy_ip and load_balancer treat substreams.

Written as characterization first (plan LP4): copy_ip should keep a substream intact on every
output, and load_balancer should treat a whole substream as one unit rather than tearing it across
workers.
"""

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
from zalfmas_fbp.components.ip.copy_ip import METADATA as COPY_META
from zalfmas_fbp.components.ip.copy_ip import Copy
from zalfmas_fbp.components.ip.load_balancer import METADATA as LB_META
from zalfmas_fbp.components.ip.load_balancer import LoadBalancer


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def run(component, messages, slots=2, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = conf(**settings)
    return run_process_component(component, inputs=inputs, outputs=(), array_outputs={"out": slots})


def shapes(result, index):
    return [str(v.type) for v in result.array_output("out")[index].values]


def contents(result, index):
    return [v.content.as_text() for v in result.array_output("out")[index].values if str(v.type) == "standard"]


SUBSTREAM = [open_bracket_message(), ip_message("a"), ip_message("b"), close_bracket_message()]


# --- copy_ip ------------------------------------------------------------------------------------


def test_copy_ip_keeps_a_substream_intact_on_every_output() -> None:
    result = run(Copy(COPY_META), SUBSTREAM)
    for index in range(2):
        assert shapes(result, index) == ["openBracket", "standard", "standard", "closeBracket"]
        assert contents(result, index) == ["a", "b"]


def test_copy_ip_copies_plain_ips_to_every_output() -> None:
    result = run(Copy(COPY_META), [ip_message("x")])
    assert all(contents(result, i) == ["x"] for i in range(2))


def test_copy_ip_preserves_nesting() -> None:
    nested = [
        open_bracket_message(),
        open_bracket_message(),
        ip_message("a"),
        close_bracket_message(),
        close_bracket_message(),
    ]
    result = run(Copy(COPY_META), nested)
    assert shapes(result, 0) == ["openBracket", "openBracket", "standard", "closeBracket", "closeBracket"]


# --- load_balancer ------------------------------------------------------------------------------


def test_load_balancer_sends_a_whole_substream_to_one_output() -> None:
    """A substream is one unit of work: tearing it across workers loses the grouping."""
    result = run(LoadBalancer(LB_META), SUBSTREAM, distribution_strategy="round_robin")

    got = [shapes(result, i) for i in range(2)]
    non_empty = [s for s in got if s]
    assert len(non_empty) == 1, f"the substream was split across outputs: {got}"
    assert non_empty[0] == ["openBracket", "standard", "standard", "closeBracket"]


def test_load_balancer_gives_successive_substreams_to_different_outputs() -> None:
    two = [
        open_bracket_message(),
        ip_message("a"),
        close_bracket_message(),
        open_bracket_message(),
        ip_message("b"),
        close_bracket_message(),
    ]
    result = run(LoadBalancer(LB_META), two, distribution_strategy="round_robin")

    assert contents(result, 0) == ["a"]
    assert contents(result, 1) == ["b"]


def test_load_balancer_still_distributes_plain_ips_one_by_one() -> None:
    result = run(
        LoadBalancer(LB_META),
        [ip_message("a"), ip_message("b")],
        distribution_strategy="round_robin",
    )
    assert contents(result, 0) == ["a"]
    assert contents(result, 1) == ["b"]


def test_load_balancer_keeps_nested_substreams_together() -> None:
    nested = [
        open_bracket_message(),
        ip_message("a"),
        open_bracket_message(),
        ip_message("b"),
        close_bracket_message(),
        close_bracket_message(),
    ]
    result = run(LoadBalancer(LB_META), nested, distribution_strategy="round_robin")

    got = [shapes(result, i) for i in range(2)]
    non_empty = [s for s in got if s]
    assert len(non_empty) == 1
    assert non_empty[0].count("openBracket") == 2
    assert non_empty[0].count("closeBracket") == 2
