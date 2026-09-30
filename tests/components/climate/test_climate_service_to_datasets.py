"""climate_service_to_datasets, converted from Runnable to Process style (plan LP3).

These are the first tests to drive a component with a live Cap'n Proto capability, served
in-process by `tests.fake_services`. Note `dataset_ids`: a capability an output IP carries is only
callable while the kj event loop is up, so reading one back has to happen in the `after` hook.
"""

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
from tests.fake_services import FakeClimateService, FakeDataset
from zalfmas_fbp.components.climate.climate_service_to_datasets import METADATA, ClimateServiceToDatasets


def run(messages, *, after=None, **settings):
    inputs: dict = {"cs": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(
        ClimateServiceToDatasets(METADATA),
        inputs=inputs,
        outputs=("ds",),
        after=after,
    )


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


async def dataset_ids(result):
    """Call info() on every dataset capability handed downstream, from inside the loop."""

    ids = []
    for value in result.output("ds").values:
        if str(value.type) != "standard":
            continue
        cap = value.content.as_interface(climate_capnp.Dataset)
        ids.append((await cap.info()).id)
    return ids


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_one_ip_per_dataset() -> None:
    service = FakeClimateService(datasets=[FakeDataset(id_="a"), FakeDataset(id_="b")])
    result = run([cap_message(lambda: service)])
    assert len(standard(result.output("ds"))) == 2
    assert service.calls.count("getAvailableDatasets") == 1


def test_the_datasets_it_sends_are_live_capabilities() -> None:
    """The whole point of the component: downstream must be able to call what it receives."""

    service = FakeClimateService(datasets=[FakeDataset(id_="ds-42"), FakeDataset(id_="ds-43")])
    result = run([cap_message(lambda: service)], after=dataset_ids)
    assert result.after_result == ["ds-42", "ds-43"]


def test_it_accepts_a_service_in_an_attribute_free_ip() -> None:
    result = run([cap_message(FakeClimateService)])
    assert len(standard(result.output("ds"))) == 1


def test_outgoing_ips_are_tagged_with_the_dataset_type() -> None:
    result = run([cap_message(FakeClimateService)])
    assert standard(result.output("ds"))[0].sysAttributes.contentType == "climate.capnp:Dataset"


def test_to_attr_puts_the_dataset_in_an_attribute_instead() -> None:
    result = run([cap_message(FakeClimateService)], to_attr="dataset")
    out = standard(result.output("ds"))[0]
    assert [entry.key for entry in out.attributes] == ["dataset"]


def test_input_attributes_are_carried_over() -> None:
    result = run([cap_message(FakeClimateService, region="north")])
    assert "region" in [entry.key for entry in standard(result.output("ds"))[0].attributes]


def test_create_substream_brackets_each_services_datasets() -> None:
    service = FakeClimateService(id_="svc-7", datasets=[FakeDataset(), FakeDataset()])
    result = run([cap_message(lambda: service)], create_substream=True)

    values = result.output("ds").values
    assert [str(v.type) for v in values] == ["openBracket", "standard", "standard", "closeBracket"]
    assert values[0].content.as_text() == "svc-7"
    assert values[-1].content.as_text() == "svc-7"


def test_no_substream_is_created_by_default() -> None:
    result = run([cap_message(FakeClimateService)])
    assert all(str(v.type) == "standard" for v in result.output("ds").values)


def test_it_does_not_ask_for_the_id_when_it_does_not_need_it() -> None:
    service = FakeClimateService()
    run([cap_message(lambda: service)])
    assert "info" not in service.calls


def test_incoming_brackets_pass_through() -> None:
    result = run(
        [open_bracket_message(), cap_message(FakeClimateService), close_bracket_message()],
    )
    assert [str(v.type) for v in result.output("ds").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_a_service_without_datasets_emits_nothing() -> None:
    result = run([cap_message(lambda: FakeClimateService(datasets=[]))])
    assert result.output("ds").values == []


def test_an_empty_service_does_not_open_a_substream_it_never_fills() -> None:
    result = run([cap_message(lambda: FakeClimateService(datasets=[]))], create_substream=True)
    assert result.output("ds").values == []


def test_an_unreadable_input_is_skipped_by_default() -> None:
    result = run([ip_message("not a service"), cap_message(FakeClimateService)])
    assert len(standard(result.output("ds"))) == 1


def test_an_unreadable_input_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no climate service"):
        run([ip_message("not a service")], on_error="fail")


def test_several_services_in_a_row() -> None:
    result = run(
        [
            cap_message(lambda: FakeClimateService(datasets=[FakeDataset()])),
            cap_message(lambda: FakeClimateService(datasets=[FakeDataset(), FakeDataset()])),
        ]
    )
    assert len(standard(result.output("ds"))) == 3
