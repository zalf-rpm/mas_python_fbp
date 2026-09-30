"""read_observed_values: narrowed error handling (plan LP5)."""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.spotpy.read_observed_values import METADATA, Component

HEADER = "crop,x,year,value,country_id\n"
ROWS = [
    "maize,,2010,1.0,1\n",
    "maize,,2011,2.0,1\n",
    "maize,,2010,3.0,2\n",
    "millet,,2010,4.0,1\n",
]


def yield_csv(tmp_path, rows=None, header=HEADER):
    path = tmp_path / "yields.csv"
    path.write_text(header + "".join(ROWS if rows is None else rows))
    return str(path)


def run(messages, tmp_path, rows=None, **settings):
    settings.setdefault("path_to_yield_data", yield_csv(tmp_path, rows))
    settings.setdefault("from_year", 2010)
    settings.setdefault("to_year", 2011)
    inputs: dict = {
        "country_ids": [*messages, done_message()],
        "conf": [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ],
    }
    component = Component(METADATA)
    return run_process_component(component, inputs=inputs, outputs=("out",)), component


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def observed(result, index=0) -> dict:
    return json.loads(standard(result.output("out"))[index].content.as_text())


def attrs(result, index=0) -> dict:
    ip = standard(result.output("out"))[index]
    return {entry.key: entry.value.as_text() for entry in ip.attributes}


def test_it_sends_the_values_for_the_named_countries(tmp_path) -> None:
    result, _ = run([ip_message("[1]")], tmp_path)
    assert observed(result) == {"1": {"2010": 1000.0, "2011": 2000.0}}


def test_values_are_converted_to_kg_per_ha(tmp_path) -> None:
    result, _ = run([ip_message("[2]")], tmp_path)
    assert observed(result)["2"]["2010"] == 3000.0


def test_missing_years_are_filled_with_the_no_data_value(tmp_path) -> None:
    result, _ = run([ip_message("[2]")], tmp_path, no_data_value=-1)
    assert observed(result)["2"] == {"2010": 3000.0, "2011": -1}


def test_only_the_configured_crop_is_sent(tmp_path) -> None:
    result, _ = run([ip_message("[1]")], tmp_path, crop="millet")
    assert observed(result) == {"1": {"2010": 4000.0, "2011": -9999}}


def test_a_single_country_id_is_accepted(tmp_path) -> None:
    result, _ = run([ip_message("1")], tmp_path)
    assert list(observed(result)) == ["1"]


def test_no_country_ids_means_every_country(tmp_path) -> None:
    result, _ = run([ip_message("[]")], tmp_path)
    assert sorted(observed(result)) == ["1", "2"]


def test_the_param_set_id_names_the_countries(tmp_path) -> None:
    result, _ = run([ip_message("[1, 2]")], tmp_path)
    assert attrs(result)["param_set_id"] == "1-2"


def test_brackets_pass_through(tmp_path) -> None:
    result, _ = run([open_bracket_message(), ip_message("[1]"), close_bracket_message()], tmp_path)
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_the_yield_file_is_read_once_for_many_ips(tmp_path) -> None:
    """It used to be re-read, sniffed and re-parsed for every incoming IP."""

    result, component = run([ip_message("[1]"), ip_message("[2]"), ip_message("[1]")], tmp_path)
    assert len(standard(result.output("out"))) == 3
    assert component._yields_from is not None  # noqa: SLF001 - the cache is the point of the test


def test_a_malformed_yield_row_does_not_lose_the_whole_file(tmp_path) -> None:
    rows = ["maize,,2010,1.0,1\n", "broken\n", "maize,,2011,2.0,1\n"]
    result, _ = run([ip_message("[1]")], tmp_path, rows=rows)
    assert observed(result) == {"1": {"2010": 1000.0, "2011": 2000.0}}


def test_a_malformed_yield_row_can_fail_the_process(tmp_path) -> None:
    rows = ["maize,,2010,1.0,1\n", "broken\n"]
    with pytest.raises(ValueError, match="readable yield row"):
        run([ip_message("[1]")], tmp_path, rows=rows, on_error="fail")


def test_an_input_that_is_not_json_is_skipped(tmp_path) -> None:
    result, _ = run([ip_message("not json"), ip_message("[1]")], tmp_path)
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_input_can_fail_the_process(tmp_path) -> None:
    with pytest.raises(ValueError, match="no country ids"):
        run([ip_message("not json")], tmp_path, on_error="fail")


def test_an_empty_input_keeps_the_ids_in_force(tmp_path) -> None:
    result, _ = run([ip_message("[2]"), ip_message("")], tmp_path)
    assert list(observed(result, 1)) == ["2"]


def test_a_missing_yield_file_sends_an_empty_result(tmp_path) -> None:
    result, _ = run([ip_message("[1]")], tmp_path, path_to_yield_data=str(tmp_path / "nope.csv"))
    assert observed(result) == {}
