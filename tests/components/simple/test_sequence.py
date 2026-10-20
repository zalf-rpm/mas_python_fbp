from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.common.values import python_from_attr, python_from_value
from zalfmas_fbp.components.simple.sequence import METADATA, Config, Sequence, elements_for


def run(triggers=None, **settings):
    ports: dict = {}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    if triggers is not None:
        ports["trigger"] = [*triggers, done_message()]
    return run_process_component(Sequence(METADATA), inputs=ports).output()


def emitted(writer):
    return [
        python_from_value(value.content.as_struct(common_capnp.Value))
        for value in writer.values
        if str(value.type) == "standard"
    ]


def shapes(writer):
    return [str(value.type) for value in writer.values]


# --- element generation ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({}, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ({"start": 2, "stop": 5}, [2, 3, 4]),
        ({"start": 0, "stop": 10, "step": 3}, [0, 3, 6, 9]),
        ({"start": 5, "stop": 0, "step": -2}, [5, 3, 1]),
        ({"start": 0, "stop": 1, "step": 0.5}, [0.0, 0.5]),
        ({"start": 0, "stop": 0}, []),
    ],
)
def test_numeric_ranges(settings, expected) -> None:
    assert elements_for(Config(**settings)) == expected


def test_whole_number_ranges_emit_integers_not_floats() -> None:
    assert all(isinstance(value, int) for value in elements_for(Config(stop=3)))
    assert all(isinstance(value, float) for value in elements_for(Config(stop=3, step=0.5)))


def test_a_zero_step_is_rejected_rather_than_looping_forever() -> None:
    with pytest.raises(ValueError, match="must not be zero"):
        elements_for(Config(step=0))


def test_literal_lists() -> None:
    assert elements_for(Config(mode="list", sequence_values=["a", 2, True])) == ["a", 2, True]


def test_date_ranges_are_inclusive_of_start_and_exclusive_of_stop() -> None:
    elements = elements_for(Config(mode="dates", date_start="2026-01-30", date_stop="2026-02-02"))
    assert elements == ["2026-01-30", "2026-01-31", "2026-02-01"]


def test_date_ranges_can_step_backwards_and_reformat() -> None:
    elements = elements_for(
        Config(
            mode="dates",
            date_start="2026-03-03",
            date_stop="2026-03-01",
            date_step_days=-1,
            date_format="%d.%m.%Y",
        ),
    )
    assert elements == ["03.03.2026", "02.03.2026"]


def test_a_date_range_needs_both_bounds() -> None:
    with pytest.raises(ValueError, match="date_start"):
        elements_for(Config(mode="dates", date_start="2026-01-01"))


# --- emission -------------------------------------------------------------------------------


def test_emits_the_sequence_immediately_without_a_trigger() -> None:
    assert emitted(run(stop=3)) == [0, 1, 2]


def test_elements_are_common_values_by_default() -> None:
    writer = run(stop=2)
    assert writer.values[0].content.as_struct(common_capnp.Value).which() == "ui8"


def test_elements_can_be_json_or_plain_text() -> None:
    as_json = run(mode="list", sequence_values=[{"a": 1}], as_type="json")
    assert json.loads(as_json.values[0].content.as_text()) == {"a": 1}

    as_text = run(mode="list", sequence_values=["plain"], as_type="text")
    assert as_text.values[0].content.as_text() == "plain"


def test_repeat_emits_the_sequence_several_times() -> None:
    assert emitted(run(stop=2, repeat=3)) == [0, 1, 0, 1, 0, 1]


def test_the_sequence_can_be_wrapped_in_a_substream() -> None:
    writer = run(stop=2, wrap_in_substream=True)
    assert shapes(writer) == ["openBracket", "standard", "standard", "closeBracket"]


def test_repeated_emissions_get_one_substream_each() -> None:
    writer = run(stop=1, repeat=2, wrap_in_substream=True)
    assert shapes(writer) == ["openBracket", "standard", "closeBracket"] * 2


def test_an_index_attribute_can_be_attached() -> None:
    writer = run(stop=3, index_attr="i")
    assert [python_from_attr(value.attributes[0]) for value in writer.values] == [0, 1, 2]


def test_on_trigger_emits_the_whole_sequence_per_trigger_ip() -> None:
    writer = run(triggers=[ip_message("go"), ip_message("go")], stop=2, emit="on_trigger")
    assert emitted(writer) == [0, 1, 0, 1]


def test_one_per_trigger_emits_a_single_element_per_trigger_ip() -> None:
    writer = run(triggers=[ip_message("go"), ip_message("go")], stop=5, emit="one_per_trigger")
    assert emitted(writer) == [0, 1]


def test_one_per_trigger_stops_when_the_sequence_runs_out() -> None:
    writer = run(triggers=[ip_message("go")] * 5, stop=2, emit="one_per_trigger")
    assert emitted(writer) == [0, 1]


def test_a_trigger_mode_without_a_connected_trigger_emits_once() -> None:
    """Better than emitting nothing at all and leaving the flow silently stalled."""
    assert emitted(run(stop=2, emit="on_trigger")) == [0, 1]


def test_an_unbuildable_sequence_finishes_quietly_instead_of_crashing() -> None:
    assert run(step=0).values == []
