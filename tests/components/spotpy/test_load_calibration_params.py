"""load_calibration_params: narrowed error handling (plan LP5)."""

from __future__ import annotations

import json

import pytest

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.spotpy.load_calibration_params import METADATA, Component

HEADER = "name,array_index,low,high,step,optguess,minbound,maxbound,derive\n"


def csv_file(tmp_path, rows, header=HEADER):
    path = tmp_path / "calibratethese.csv"
    path.write_text(header + "".join(rows))
    return str(path)


def run(path, **settings):
    from mas.schema.common import common_capnp

    settings["path_to_calibrate_csv"] = path
    inputs: dict = {
        "conf": [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    }
    return run_process_component(Component(METADATA), inputs=inputs, outputs=("params",))


def params_of(result) -> list[dict]:
    return json.loads(result.output("params").values[0].content.as_text())


def test_a_plain_row_becomes_a_parameter(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,0.1,0.9,,0.5,,\n"])
    assert params_of(run(path)) == [{"name": "pA", "low": 0.1, "high": 0.9, "optguess": 0.5}]


def test_an_array_index_is_read_as_an_integer(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,3,0.1,0.9,,,,\n"])
    assert params_of(run(path))[0]["array_index"] == 3


def test_all_numeric_columns_are_read(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,1,2,0.5,1.5,0.9,2.1,\n"])
    assert params_of(run(path))[0] == {
        "name": "pA",
        "low": 1.0,
        "high": 2.0,
        "step": 0.5,
        "optguess": 1.5,
        "minbound": 0.9,
        "maxbound": 2.1,
    }


def test_a_derive_expression_does_not_stop_the_component_sending_anything(tmp_path) -> None:
    """It used to be built as a lambda, which made the whole list unserialisable - so a single
    row with a ninth column meant the component emitted nothing at all."""

    path = csv_file(tmp_path, ["pA,,0.1,0.9,,,,,x*2\n"])
    assert params_of(run(path)) == [{"name": "pA", "low": 0.1, "high": 0.9, "derive_expression": "x*2"}]


def test_each_row_keeps_its_own_derive_expression(tmp_path) -> None:
    """The lambda closed over the loop variable, so every row got the last row's expression."""

    path = csv_file(tmp_path, ["pA,,0,1,,,,,x*2\n", "pB,,0,1,,,,,x+9\n"])
    assert [p["derive_expression"] for p in params_of(run(path))] == ["x*2", "x+9"]


def test_several_rows(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,0,1,,,,\n", "pB,,2,3,,,,\n", "pC,,4,5,,,,\n"])
    assert [p["name"] for p in params_of(run(path))] == ["pA", "pB", "pC"]


def test_a_semicolon_file_is_read_too(tmp_path) -> None:
    path = csv_file(
        tmp_path,
        ["pA;;0.1;0.9;;;;\n"],
        header="name;array_index;low;high;step;optguess;minbound;maxbound\n",
    )
    assert params_of(run(path))[0]["name"] == "pA"


def test_the_delimiter_can_be_set_explicitly(tmp_path) -> None:
    path = csv_file(
        tmp_path,
        ["pA\t\t0.1\t0.9\t\t\t\t\n"],
        header="name\tarray_index\tlow\thigh\tstep\toptguess\tminbound\tmaxbound\n",
    )
    assert params_of(run(path, delimiter="\t"))[0]["name"] == "pA"


def test_an_unsniffable_file_falls_back_to_a_comma(tmp_path) -> None:
    """csv.Sniffer raises on plenty of good files; giving up meant loading no parameters."""

    path = csv_file(tmp_path, ["pA,,0.1,0.9,,,,\n"])
    assert params_of(run(path))[0]["name"] == "pA"


def test_a_short_row_is_skipped_but_the_rest_are_kept(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,0,1,,,,\n", "broken\n", "pC,,4,5,,,,\n"])
    assert [p["name"] for p in params_of(run(path))] == ["pA", "pC"]


def test_a_non_numeric_value_is_skipped(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,not-a-number,1,,,,\n", "pB,,2,3,,,,\n"])
    assert [p["name"] for p in params_of(run(path))] == ["pB"]


def test_a_bad_row_can_fail_the_process(tmp_path) -> None:
    path = csv_file(tmp_path, ["pA,,not-a-number,1,,,,\n"])
    with pytest.raises(ValueError, match="readable parameter row"):
        run(path, on_error="fail")


def test_a_missing_file_sends_an_empty_list(tmp_path) -> None:
    result = run(str(tmp_path / "nope.csv"))
    assert params_of(result) == []


def test_a_missing_file_can_fail_the_process(tmp_path) -> None:
    with pytest.raises(OSError, match="nope.csv"):
        run(str(tmp_path / "nope.csv"), on_error="fail")


def test_an_empty_file_sends_an_empty_list(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("")
    assert params_of(run(str(path))) == []
