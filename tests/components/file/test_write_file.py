from __future__ import annotations

from pathlib import Path
from typing import Any

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import PortMessage, PortValue, done_message, ip_message, run_process_component
from zalfmas_fbp.components.file.write_file import METADATA as write_file_metadata
from zalfmas_fbp.components.file.write_file import WriteFile


def _ip_with_attrs(content: str, **attrs: Any) -> PortMessage:
    attributes = [{"key": key, "value": value} for key, value in attrs.items()]
    return PortMessage(PortValue(fbp_capnp.IP.new_message(content=content, attributes=attributes)))


def test_write_file_uses_attribute_referenced_via_at_prefix_in_filename_pattern(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path), "filename_pattern": "csv_{@id}.csv"})

    run_process_component(
        component,
        inputs={"in": [_ip_with_attrs("hello", id="42"), done_message()]},
        outputs=(),
    )

    assert (tmp_path / "csv_42.csv").read_text() == "hello"


def test_write_file_supports_multiple_attribute_placeholders(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "{@year}_{@id}_{@year}.csv"},
    )

    run_process_component(
        component,
        inputs={"in": [_ip_with_attrs("hello", year="2026", id="42"), done_message()]},
        outputs=(),
    )

    assert (tmp_path / "2026_42_2026.csv").read_text() == "hello"


def test_write_file_reads_numeric_common_value_attribute(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path), "filename_pattern": "csv_{@id}.csv"})

    run_process_component(
        component,
        inputs={
            "in": [_ip_with_attrs("hello", id=common_capnp.Value.new_message(i64=42)), done_message()],
        },
        outputs=(),
    )

    assert (tmp_path / "csv_42.csv").read_text() == "hello"


def test_write_file_defaults_to_running_count_in_filename(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values({"path_to_out_dir": str(tmp_path)})

    run_process_component(
        component,
        inputs={"in": [ip_message("first"), ip_message("second"), done_message()]},
        outputs=(),
    )

    assert (tmp_path / "csv_0.csv").read_text() == "first"
    assert (tmp_path / "csv_1.csv").read_text() == "second"


def test_write_file_combines_attribute_and_count_placeholders(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "{@id}_{count}.csv"},
    )

    run_process_component(
        component,
        inputs={
            "in": [
                _ip_with_attrs("first", id="a"),
                _ip_with_attrs("second", id="a"),
                done_message(),
            ],
        },
        outputs=(),
    )

    assert (tmp_path / "a_0.csv").read_text() == "first"
    assert (tmp_path / "a_1.csv").read_text() == "second"


def test_write_file_skips_ip_missing_referenced_attribute_but_keeps_counting(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "{@id}_{count}.csv"},
    )

    run_process_component(
        component,
        inputs={
            "in": [
                ip_message("no-id-attr"),
                _ip_with_attrs("has-id", id="a"),
                done_message(),
            ],
        },
        outputs=(),
    )

    assert [p.name for p in tmp_path.iterdir()] == ["a_1.csv"]
    assert (tmp_path / "a_1.csv").read_text() == "has-id"


def test_write_file_skips_ip_on_unknown_placeholder(tmp_path: Path) -> None:
    component = WriteFile(write_file_metadata)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "{not_count_or_attr}.csv"},
    )

    run_process_component(
        component,
        inputs={"in": [ip_message("hello"), done_message()]},
        outputs=(),
    )

    assert list(tmp_path.iterdir()) == []


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
