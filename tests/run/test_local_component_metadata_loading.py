from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from zalfmas_fbp.run import local_components_service
from zalfmas_fbp.run.metadata import ComponentMetadata


def _flat_standard_metadata(*, category: dict[str, str] | None = None) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "info": {
            "id": "console-output",
            "name": "output to console",
            "description": "Output input to console.",
        },
        "type": "standard",
        "inPorts": [{"name": "in"}],
        "outPorts": [],
    }
    if category is not None:
        metadata["category"] = category
    return metadata


def _stub_registry_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyComponentModule:
        @staticmethod
        def new_message(**kwargs: Any) -> dict[str, Any]:
            return kwargs

    class DummyEntryModule:
        @staticmethod
        def new_message(**kwargs: Any) -> dict[str, Any]:
            return kwargs

    monkeypatch.setattr(local_components_service.fbp_capnp, "Component", DummyComponentModule)
    monkeypatch.setattr(local_components_service.registry_capnp, "Registry", SimpleNamespace(Entry=DummyEntryModule))
    monkeypatch.setattr(local_components_service.common, "IdentifiableHolder", lambda value: value)


@pytest.mark.parametrize(
    ("cached_metadata", "expected_cat_id", "expected_cat_name"),
    [
        pytest.param(
            _flat_standard_metadata(category={"id": "console", "name": "Console"}),
            "console",
            "Console",
            id="categorized",
        ),
        pytest.param(_flat_standard_metadata(), "uncategorized", "Uncategorized", id="uncategorized"),
    ],
)
def test_load_component_metadata_accepts_flat_cached_metadata(
    cached_metadata: dict[str, Any],
    expected_cat_id: str,
    expected_cat_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_registry_builders(monkeypatch)

    result = local_components_service.load_component_metadata(
        {"console-output": "python /tmp/console-output.py"},
        {"console-output": cached_metadata},
        restorer=object(),
    )

    assert result[expected_cat_id]["name"] == expected_cat_name
    assert len(result[expected_cat_id]["component_holders"]) == 1


def test_load_component_metadata_refreshes_legacy_cached_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_registry_builders(monkeypatch)

    stdout = json.dumps(_flat_standard_metadata(category={"id": "console", "name": "Console"}))

    def fake_run(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(local_components_service.sp, "run", fake_run)

    components_cache: dict[str, Any] = {
        "console-output": {
            "category": {"id": "console", "name": "Console"},
            "component": {
                "info": {
                    "id": "console-output",
                    "name": "output to console",
                    "description": "Output input to console.",
                },
                "type": "standard",
                "inPorts": [{"name": "in"}],
                "outPorts": [],
            },
        },
    }
    result = local_components_service.load_component_metadata(
        {"console-output": "python /tmp/console-output.py"},
        components_cache,
        restorer=object(),
    )

    assert result["console"]["name"] == "Console"
    assert components_cache["console-output"]["type"] == "standard"
    assert "component" not in components_cache["console-output"]


def test_checked_in_local_components_cache_uses_canonical_metadata() -> None:
    cache_path = Path(__file__).resolve().parents[2] / "configs" / "local_components_cache.json"
    components_cache = json.loads(cache_path.read_text())

    assert components_cache
    for comp_id, metadata_json in components_cache.items():
        assert "component" not in metadata_json
        metadata = ComponentMetadata.model_validate(metadata_json)
        assert metadata.info.id == comp_id
