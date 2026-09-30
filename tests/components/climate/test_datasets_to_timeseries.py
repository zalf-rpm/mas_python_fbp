"""datasets_to_timeseries, converted from Runnable to Process style (plan LP3)."""

from __future__ import annotations

import json

import pytest
from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp

from tests.component_harness import (
    cap_message,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from tests.fake_services import FakeDataset, FakeLocation, FakeTimeSeries
from zalfmas_fbp.components.climate.datasets_to_timeseries import METADATA, DatasetsToTimeseries


def run(messages, *, after=None, **settings):
    inputs: dict = {"ds": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(
        DatasetsToTimeseries(METADATA),
        inputs=inputs,
        outputs=("ts",),
        after=after,
    )


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def attrs_of(ip) -> dict:
    return {entry.key: entry for entry in ip.attributes}


async def timeseries_ids(result):
    ids = []
    for value in standard(result.output("ts")):
        cap = value.content.as_interface(climate_capnp.TimeSeries)
        ids.append((await cap.info()).id)
    return ids


def grid_dataset(n=3, **kwargs):
    locations = [FakeLocation(id_=f"loc-{i}", row_col=(0, i)) for i in range(n)]
    return FakeDataset(locations=locations, **kwargs)


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_one_ip_per_location() -> None:
    result = run([cap_message(lambda: grid_dataset(3))])
    assert len(standard(result.output("ts"))) == 3


def test_the_timeseries_it_sends_are_live_capabilities() -> None:
    dataset = FakeDataset(
        locations=[
            FakeLocation(id_="a", time_series=FakeTimeSeries(id_="ts-a")),
            FakeLocation(id_="b", time_series=FakeTimeSeries(id_="ts-b")),
        ]
    )
    result = run([cap_message(lambda: dataset)], after=timeseries_ids)
    assert result.after_result == ["ts-a", "ts-b"]


def test_outgoing_ips_are_tagged_with_the_timeseries_type() -> None:
    result = run([cap_message(lambda: grid_dataset(1))])
    assert standard(result.output("ts"))[0].sysAttributes.contentType == "climate.capnp:TimeSeries"


def test_a_grid_location_is_identified_by_row_and_col() -> None:
    dataset = FakeDataset(locations=[FakeLocation(id_="ignored", row_col=(7, 9))])
    result = run([cap_message(lambda: dataset)])
    entry = attrs_of(standard(result.output("ts"))[0])["id"]
    assert entry.value.as_struct(common_capnp.Value).t == "row-7_col-9"


def test_a_location_without_a_row_col_falls_back_to_its_own_id() -> None:
    """A non-grid dataset used to raise IndexError here, which aborted the whole dataset."""

    dataset = FakeDataset(locations=[FakeLocation(id_="station-42", row_col=None)])
    result = run([cap_message(lambda: dataset)])
    entry = attrs_of(standard(result.output("ts"))[0])["id"]
    assert entry.value.as_struct(common_capnp.Value).t == "station-42"


def test_a_dataset_of_mixed_locations_is_not_aborted_by_one_without_a_row_col() -> None:
    dataset = FakeDataset(
        locations=[
            FakeLocation(id_="a", row_col=(1, 1)),
            FakeLocation(id_="b", row_col=None),
            FakeLocation(id_="c", row_col=(2, 2)),
        ]
    )
    result = run([cap_message(lambda: dataset)])
    ids = [attrs_of(ip)["id"].value.as_struct(common_capnp.Value).t for ip in standard(result.output("ts"))]
    assert ids == ["row-1_col-1", "b", "row-2_col-2"]


def test_id_attr_can_be_renamed_or_switched_off() -> None:
    result = run([cap_message(lambda: grid_dataset(1))], id_attr="location")
    assert "location" in attrs_of(standard(result.output("ts"))[0])

    result = run([cap_message(lambda: grid_dataset(1))], id_attr="")
    assert attrs_of(standard(result.output("ts"))[0]) == {}


def test_to_attr_puts_the_timeseries_in_an_attribute_instead() -> None:
    result = run([cap_message(lambda: grid_dataset(1))], to_attr="timeseries")
    out = standard(result.output("ts"))[0]
    assert "timeseries" in attrs_of(out)
    assert not out.sysAttributes.contentType


def test_locations_are_fetched_in_pages() -> None:
    """5 locations at 2 a page: 2, 2, 1, then one more call to see the empty page that ends it."""

    dataset = grid_dataset(5)
    result = run([cap_message(lambda: dataset)], no_of_locations_at_once=2)
    assert dataset.callbacks[0].requested_counts == [2, 2, 2, 2]
    assert len(standard(result.output("ts"))) == 5


def test_streaming_can_resume_after_a_location() -> None:
    dataset = FakeDataset(locations=[FakeLocation(id_=f"loc-{i}", row_col=None) for i in range(4)])
    result = run([cap_message(lambda: dataset)], continue_after_location_id="loc-1")
    ids = [attrs_of(ip)["id"].value.as_struct(common_capnp.Value).t for ip in standard(result.output("ts"))]
    assert ids == ["loc-2", "loc-3"]
    assert dataset.stream_started_after == ["loc-1"]


def test_wrap_in_substream_brackets_each_datasets_timeseries() -> None:
    result = run([cap_message(lambda: grid_dataset(2, id_="ds-7"))], wrap_in_substream=True)
    values = result.output("ts").values
    assert [str(v.type) for v in values] == ["openBracket", "standard", "standard", "closeBracket"]
    assert values[0].content.as_text() == "ds-7"


def test_a_dataset_without_locations_leaves_no_empty_substream() -> None:
    result = run([cap_message(lambda: FakeDataset(locations=[]))], wrap_in_substream=True)
    assert result.output("ts").values == []


def test_incoming_brackets_pass_through() -> None:
    """Transparent by default, like every other Process component. Compose 'Flatten substreams'
    upstream to drop them, rather than configuring it here."""

    result = run([open_bracket_message(), cap_message(lambda: grid_dataset(1)), close_bracket_message()])
    assert [str(v.type) for v in result.output("ts").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_a_close_bracket_is_not_mistaken_for_a_dataset() -> None:
    """It used to fall through the bracket check and be read as a dataset, logging an exception."""

    dataset = grid_dataset(1)
    result = run([close_bracket_message(), cap_message(lambda: dataset)])
    assert len(standard(result.output("ts"))) == 1


def test_input_attributes_are_carried_over() -> None:
    result = run([cap_message(lambda: grid_dataset(1), scenario="rcp85")])
    assert "scenario" in attrs_of(standard(result.output("ts"))[0])


def test_an_unreadable_input_is_skipped_by_default() -> None:
    result = run([ip_message("not a dataset"), cap_message(lambda: grid_dataset(1))])
    assert len(standard(result.output("ts"))) == 1


def test_an_unreadable_input_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no climate dataset"):
        run([ip_message("not a dataset")], on_error="fail")


def test_it_does_not_ask_for_the_id_when_it_does_not_need_it() -> None:
    dataset = grid_dataset(1)
    run([cap_message(lambda: dataset)])
    assert "info" not in dataset.calls


def test_several_datasets_in_a_row() -> None:
    result = run([cap_message(lambda: grid_dataset(2)), cap_message(lambda: grid_dataset(3))])
    assert len(standard(result.output("ts"))) == 5
