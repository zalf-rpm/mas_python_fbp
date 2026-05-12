from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from mas.schema.common import common_capnp
from pydantic import ValidationError

from zalfmas_fbp.run.metadata import ComponentMetadata
from zalfmas_fbp.run.process import Process, ProcessConfig
from zalfmas_fbp.run.process.config.config_codec import (
    config_value_from_python,
    python_value_from_capnp_value,
)


class _TypedConfig(ProcessConfig):
    split_at: str = ","


class _TypedProcess(Process[_TypedConfig]):
    pass


def test_config_value_round_trips_nested_python_values() -> None:
    original = {
        "name": "alpha",
        "count": 3,
        "enabled": True,
        "weights": [1.0, 2.5],
        "nested": {"labels": ["a", "b"], "mixed": [1, True, {"leaf": "value"}]},
    }

    capnp_value = config_value_from_python(original)

    assert python_value_from_capnp_value(capnp_value.as_reader()) == original


def test_config_value_rejects_non_string_dict_keys() -> None:
    with pytest.raises(TypeError, match="Config dict keys must be strings"):
        config_value_from_python({1: "value"})


def test_python_value_from_capnp_rejects_malformed_pair_entries() -> None:
    malformed = common_capnp.Value.new_message(
        lpair=[
            common_capnp.Pair.new_message(fst="missing-snd"),
        ],
    )

    with pytest.raises(TypeError, match="both 'fst' and 'snd'"):
        python_value_from_capnp_value(malformed.as_reader())


def test_process_without_config_model_keeps_raw_dict_config() -> None:
    component = Process(metadata=_metadata_with_split_config())

    component.apply_config_values({"split_at": ";"})

    assert component.config["split_at"] == ";"
    assert component.raw_config["split_at"] == ";"


def test_process_with_config_model_exposes_typed_config() -> None:
    component = _TypedProcess(metadata=_metadata_with_split_config())

    component.apply_config_values({"split_at": ";"})

    assert component.config.split_at == ";"
    assert component.raw_config["split_at"] == ";"


def test_rpc_config_entry_validates_typed_config_and_preserves_previous_value() -> None:
    component = _TypedProcess(metadata=_metadata_with_split_config())

    asyncio.run(
        component.setConfigEntry(
            "split_at",
            common_capnp.Value.new_message(t=";"),
            cast(Any, None),
        ),
    )

    assert component.config.split_at == ";"

    with pytest.raises(ValidationError, match="split_at"):
        asyncio.run(
            component.setConfigEntry(
                "split_at",
                common_capnp.Value.new_message(i64=3),
                cast(Any, None),
            ),
        )

    assert component.config.split_at == ";"
    assert component.raw_config["split_at"] == ";"


def test_process_init_propagates_bootstrap_metadata_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    metadata = _metadata_with_split_config()

    def raise_bootstrap_error(self: ComponentMetadata) -> dict[str, object]:
        raise ValueError("boom")

    monkeypatch.setattr(ComponentMetadata, "default_config_values", raise_bootstrap_error)

    with pytest.raises(ValueError, match="boom"):
        Process(metadata=metadata)


def _metadata_with_split_config() -> ComponentMetadata:
    return ComponentMetadata.model_validate(
        {
            "info": {"id": "typed-config-test", "name": "typed config test"},
            "type": "process",
            "defaultConfig": {"split_at": {"value": ",", "type": "string"}},
        },
    )
