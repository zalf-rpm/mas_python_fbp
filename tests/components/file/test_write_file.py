from __future__ import annotations

from pathlib import Path

from mas.schema.fbp import fbp_capnp

from tests.component_harness import PortMessage, PortValue, done_message, ip_message, run_process_component
from zalfmas_fbp.components.file.write_file import METADATA as write_file_metadata
from zalfmas_fbp.components.file.write_file import WriteFile


def _ip_with_id(content: str, id_: str) -> PortMessage:
    return PortMessage(
        PortValue(fbp_capnp.IP.new_message(content=content, attributes=[{"key": "id", "value": id_}])),
    )


def test_write_file_writes_content_using_id_attr_and_filepath_pattern(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path)})

    run_process_component(
        component,
        inputs={"in": [_ip_with_id("hello", "42"), done_message()]},
        outputs=(),
    )

    assert (tmp_path / "csv_42.csv").read_text() == "hello"


def test_write_file_falls_back_to_running_count_without_id_attr(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path)})

    run_process_component(
        component,
        inputs={"in": [ip_message("first"), ip_message("second"), done_message()]},
        outputs=(),
    )

    assert (tmp_path / "csv_0.csv").read_text() == "first"
    assert (tmp_path / "csv_1.csv").read_text() == "second"


def test_write_file_appends_when_configured(tmp_path: Path) -> None:
    target = tmp_path / "csv_0.csv"
    target.write_text("existing-")
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path), "append": True})

    run_process_component(
        component,
        inputs={"in": [ip_message("new"), done_message()]},
        outputs=(),
    )

    assert target.read_text() == "existing-new"


def test_write_file_does_not_create_missing_dirs_by_default(tmp_path: Path) -> None:
    missing_dir = tmp_path / "missing"
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(missing_dir)})

    run_process_component(
        component,
        inputs={"in": [ip_message("hello"), done_message()]},
        outputs=(),
    )

    assert not missing_dir.exists()


def test_write_file_creates_missing_dirs_when_configured(tmp_path: Path) -> None:
    missing_dir = tmp_path / "missing" / "nested"
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(missing_dir), "create_missing_dirs": True})

    run_process_component(
        component,
        inputs={"in": [ip_message("hello"), done_message()]},
        outputs=(),
    )

    assert (missing_dir / "csv_0.csv").read_text() == "hello"
