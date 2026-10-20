"""Structural checks over every component in the tree.

A duplicate info.id made climate/timeseries_data_to_monthly_aggregate unlaunchable for some time:
local_cmds.json is keyed by id, so only one of the two colliding components could be registered, and
nothing noticed because neither had a test. These checks are cheap and close that class of mistake.
"""

from __future__ import annotations

import ast
import json
import pathlib
from collections import defaultdict

COMPONENTS = pathlib.Path("zalfmas_fbp/components")
CONFIGS = pathlib.Path("zalfmas_fbp/configs")


def _metadata_of(path: pathlib.Path) -> dict[str, str] | None:
    """id, name and type straight from the source, without importing it."""
    for node in ast.walk(ast.parse(path.read_text())):
        if not (isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "METADATA" for t in node.targets)):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        kwargs = {k.arg: k.value for k in node.value.keywords}

        def literal(key: str, field: str) -> str | None:
            value = kwargs.get(key)
            if isinstance(value, ast.Call):
                for k in value.keywords:
                    if k.arg == field and isinstance(k.value, ast.Constant):
                        return str(k.value.value)
            return None

        component_id = literal("info", "id")
        if component_id is None:
            return None
        kind = kwargs.get("type")
        return {
            "id": component_id,
            "name": literal("info", "name") or "",
            "type": kind.value if isinstance(kind, ast.Constant) else "",
            "module": str(path.with_suffix("")).replace("/", "."),
        }
    return None


def all_components() -> list[dict[str, str]]:
    """Every real component. Templates are excluded: they carry placeholder metadata on purpose and
    are neither registered nor in the cache.
    """
    found = []
    for path in sorted(COMPONENTS.rglob("*.py")):
        if "__pycache__" in str(path) or path.name == "__init__.py" or "component_templates" in str(path):
            continue
        metadata = _metadata_of(path)
        if metadata is not None:
            found.append(metadata)
    return found


def registered() -> dict[str, str]:
    return {k: v for k, v in json.loads((CONFIGS / "local_cmds.json").read_text()).items() if len(k) == 36}


def test_the_tree_has_components_to_check() -> None:
    """Guards the checks below against silently passing on an empty list."""
    assert len(all_components()) > 50
    assert not any("component_templates" in c["module"] for c in all_components())


def test_every_component_id_is_unique() -> None:
    by_id: dict[str, list[str]] = defaultdict(list)
    for component in all_components():
        by_id[component["id"]].append(component["module"])

    duplicates = {cid: modules for cid, modules in by_id.items() if len(modules) > 1}
    assert not duplicates, f"local_cmds.json is keyed by id, so these cannot both be launched: {duplicates}"


def test_every_component_name_is_unique() -> None:
    """Two components with one name are indistinguishable in the flow editor."""
    by_name: dict[str, list[str]] = defaultdict(list)
    for component in all_components():
        by_name[component["name"]].append(component["module"])

    duplicates = {name: modules for name, modules in by_name.items() if len(modules) > 1}
    assert not duplicates, f"duplicate component names: {duplicates}"


def test_every_registered_command_points_at_a_module_that_exists() -> None:
    modules = {component["module"] for component in all_components()}
    broken = {cid: cmd for cid, cmd in registered().items() if cmd.split()[-1] not in modules}
    assert not broken, f"local_cmds.json entries whose module is gone: {broken}"


def test_every_registered_id_belongs_to_the_module_it_names() -> None:
    by_module = {component["module"]: component["id"] for component in all_components()}
    mismatched = {cid: cmd for cid, cmd in registered().items() if by_module.get(cmd.split()[-1]) not in (None, cid)}
    assert not mismatched, f"local_cmds.json ids that do not match their module's METADATA: {mismatched}"


def test_the_components_cache_matches_the_registered_commands() -> None:
    cache = json.loads((CONFIGS / "local_components_cache.json").read_text())
    missing = set(registered()) - set(cache)
    assert not missing, f"registered but not in the cache; regenerate it: {sorted(missing)}"


def test_component_ids_look_like_uuids() -> None:
    """agents_process.md requires a UUID4, and a collision is what made a component unlaunchable."""
    import uuid

    bad = []
    for component in all_components():
        try:
            _ = uuid.UUID(component["id"])
        except ValueError:
            bad.append((component["module"], component["id"]))
    assert not bad, f"info.id must be a UUID: {bad}"
