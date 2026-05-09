from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from zalfmas_fbp.run.metadata import ComponentMetadata


def _standard_component_modules() -> list[str]:
    components_root = Path(__file__).resolve().parents[2] / "zalfmas_fbp" / "components"
    modules: list[str] = []
    for path in sorted(components_root.rglob("*.py")):
        text = path.read_text()
        if "run_component_from_metadata(run_component, METADATA)" not in text:
            continue
        modules.append(".".join(path.relative_to(components_root.parent.parent).with_suffix("").parts))
    return modules


@pytest.mark.parametrize("module_name", _standard_component_modules())
def test_standard_component_modules_expose_typed_metadata(module_name: str) -> None:
    module = importlib.import_module(module_name)
    metadata = module.METADATA

    assert isinstance(metadata, ComponentMetadata)
    assert metadata.type == "standard"
    assert metadata.info.id
