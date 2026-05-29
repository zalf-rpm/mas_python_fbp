from __future__ import annotations

import json
import sys
from typing import Any

import pytest

from zalfmas_fbp.run import components
from zalfmas_fbp.run.metadata import ComponentMetadata


def _metadata() -> ComponentMetadata:
    return ComponentMetadata.model_validate(
        {
            "info": {
                "id": "56a0d4bc-5f58-4c1c-a5ef-806dc2719c7e",
                "name": "demo component",
                "description": "Demo standard component.",
            },
            "type": "standard",
            "inPorts": [{"name": "in"}],
            "defaultConfig": {"split_at": {"value": ",", "type": "string"}},
        },
    )


def test_run_component_from_metadata_uses_metadata_name_by_default(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    async def run_component(port_infos_reader_sr: str, config: dict[str, Any]) -> None:
        captured["port_infos_reader_sr"] = port_infos_reader_sr
        captured["config"] = config

    monkeypatch.setattr(sys, "argv", ["component.py", "reader-sr"])

    components.run_component_from_metadata(run_component, _metadata())

    assert captured["port_infos_reader_sr"] == "reader-sr"
    assert captured["config"] == {"split_at": ",", "name": "demo component"}


def test_run_component_from_metadata_prefers_runtime_name_argument(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    async def run_component(port_infos_reader_sr: str, config: dict[str, Any]) -> None:
        captured["port_infos_reader_sr"] = port_infos_reader_sr
        captured["config"] = config

    monkeypatch.setattr(sys, "argv", ["component.py", "reader-sr", "--name", "runtime demo"])

    components.run_component_from_metadata(run_component, _metadata())

    assert captured["port_infos_reader_sr"] == "reader-sr"
    assert captured["config"] == {"split_at": ",", "name": "runtime demo"}


def test_output_json_default_config_omits_runtime_name(monkeypatch: Any, capsys: Any) -> None:
    metadata = _metadata()
    parser = components.create_default_fbp_component_args_parser(metadata.info.description)
    monkeypatch.setattr(sys, "argv", ["component.py", "--output_json_default_config", "--name", "runtime demo"])

    with pytest.raises(SystemExit) as exc_info:
        components.handle_default_fpb_component_args(parser, metadata)

    assert exc_info.value.code == 0
    assert json.loads(capsys.readouterr().out) == {"split_at": ","}
