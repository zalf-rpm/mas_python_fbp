"""read_excel: the out port and nested sending."""

from __future__ import annotations

import json
from pathlib import Path

import pandas
from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.file.read_excel import METADATA, Component

NESTING = {"experiments": ["EID"], "treatments": ["EID", "TREAT_ID"]}


def write_workbook(tmp_path: Path) -> str:
    path = tmp_path / "trial.xlsx"
    with pandas.ExcelWriter(path) as writer:
        pandas.DataFrame({"EID": ["E1", "E2"], "SITE": ["north", "south"]}).to_excel(
            writer, sheet_name="Sites", index=False
        )
        pandas.DataFrame({"EID": ["E1", "E1", "E2"], "TREAT_ID": ["T1", "T2", "T3"], "N": [10, 20, 30]}).to_excel(
            writer, sheet_name="Fert", index=False
        )
    return str(path)


def conf(**settings):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings)))


def run(tmp_path: Path, component: Component | None = None, **settings):
    settings = {"file": write_workbook(tmp_path), "load_all": True, "nesting_by_key_cols": NESTING, **settings}
    return run_process_component(
        component or Component(METADATA),
        inputs={"conf": [conf(**settings), done_message()]},
        outputs=("out",),
    ).output()


def stream(writer) -> list:
    """The output as "(" / ")" for brackets and parsed JSON for content."""
    shape = {"openBracket": "(", "closeBracket": ")"}
    return [shape.get(str(v.type)) or json.loads(v.content.as_text()) for v in writer.values]


def test_the_out_port_is_declared() -> None:
    assert "out" in [p.name for p in METADATA.outPorts]


def test_without_nesting_everything_is_one_message(tmp_path: Path) -> None:
    (only,) = stream(run(tmp_path))
    assert only["experiments"]["E1"]["sites"] == {"EID": "E1", "SITE": "north"}
    assert set(only["experiments"]["E1"]["treatments"]) == {"T1", "T2"}


def test_one_message_per_entry_of_the_innermost_level(tmp_path: Path) -> None:
    """Used to send nothing at all unless include_branch_level_data was set as well."""
    out = stream(run(tmp_path, send_nesting_level=["experiments", "treatments"]))
    assert [x if isinstance(x, str) else x["fert"]["TREAT_ID"] for x in out] == [
        "(",
        "(",
        "T1",
        "T2",
        ")",
        "(",
        "T3",
        ")",
        ")",
    ]


def test_levels_can_be_named_by_their_key_column(tmp_path: Path) -> None:
    by_name = stream(run(tmp_path, send_nesting_level=["experiments", "treatments"]))
    assert stream(run(tmp_path, send_nesting_level=["EID", "TREAT_ID"])) == by_name


def test_brackets_can_be_left_out(tmp_path: Path) -> None:
    out = stream(run(tmp_path, send_nesting_level=["EID"], include_brackets_when_sending_nested=False))
    assert [x["sites"]["SITE"] for x in out] == ["north", "south"]


def test_branch_level_data_comes_before_its_children(tmp_path: Path) -> None:
    out = stream(
        run(
            tmp_path,
            send_nesting_level=["EID", "TREAT_ID"],
            include_branch_level_data=True,
            include_brackets_when_sending_nested=False,
        )
    )
    labels = [x["sites"]["SITE"] if "sites" in x else x["fert"]["TREAT_ID"] for x in out]
    assert labels == ["north", "T1", "T2", "south", "T3"]


def test_a_level_absent_from_the_data_is_sent_whole(tmp_path: Path) -> None:
    out = stream(run(tmp_path, send_nesting_level=["EID", "PLTID"], include_brackets_when_sending_nested=False))
    assert [set(x) for x in out] == [{"sites", "treatments"}, {"sites", "treatments"}]


def test_sending_stops_once_the_out_port_is_closed(tmp_path: Path) -> None:
    """A failed write used to end only the innermost call; the rest of the tree was still sent."""
    component = Component(METADATA)
    attempts = []

    async def write_out(_name, _ip):
        attempts.append(1)
        return len(attempts) < 3

    component.write_out = write_out  # type: ignore[method-assign]
    run(tmp_path, component, send_nesting_level=["EID", "TREAT_ID"])
    assert len(attempts) == 3
