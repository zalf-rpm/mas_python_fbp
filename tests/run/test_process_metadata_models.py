from __future__ import annotations

import pytest
from pydantic import Field, ValidationError

from zalfmas_fbp.components.console.console_output import METADATA as console_output_metadata
from zalfmas_fbp.components.dakis.create_empty_raster import (
    METADATA as create_empty_raster_metadata,
)
from zalfmas_fbp.components.dakis.filter_geoparquet_by_raster import (
    METADATA as filter_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.relabel_geoparquet import (
    METADATA as relabel_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.write_geoparquet import (
    METADATA as write_geoparquet_metadata,
)
from zalfmas_fbp.components.ip.copy_ip import METADATA as copy_metadata
from zalfmas_fbp.components.ip.load_balancer import METADATA as load_balancer_metadata
from zalfmas_fbp.components.string.split_string2 import METADATA as split_string_metadata
from zalfmas_fbp.components.string.to_string import METADATA as to_string_metadata
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run.metadata import ComponentMetadata
from zalfmas_fbp.run.process import ProcessConfig


class _InlineConfig(ProcessConfig):
    split_at: str = Field(",", description="Split delimiter.")


def test_component_metadata_types_descriptive_fields() -> None:
    metadata = ComponentMetadata.model_validate(
        {
            "info": {
                "id": "component-id",
                "name": "typed metadata test",
                "description": "Verifies typed metadata fields.",
            },
            "type": "process",
            "inPorts": [{"name": "in", "contentType": "Text", "desc": "Input text."}],
            "outPorts": [{"name": "out", "type": "array", "contentType": "Text", "desc": "Output text."}],
            "defaultConfig": {
                "split_at": {
                    "value": ",",
                    "type": "string",
                    "desc": "Split delimiter.",
                },
            },
        },
    )

    assert metadata.type == "process"
    assert metadata.category is None
    assert metadata.info.id == "component-id"
    assert metadata.inPorts[0].desc == "Input text."
    assert metadata.outPorts[0].type == "array"
    assert metadata.defaultConfig["split_at"].type == "string"
    assert metadata.defaultConfig["split_at"].desc == "Split delimiter."


def test_component_metadata_rejects_unknown_metadata_keys() -> None:
    with pytest.raises(ValidationError, match="idd"):
        ComponentMetadata.model_validate(
            {
                "info": {
                    "idd": "component-id",
                    "name": "typed metadata test",
                },
                "type": "process",
            },
        )


def test_component_metadata_requires_info_id() -> None:
    with pytest.raises(ValidationError, match="id"):
        ComponentMetadata.model_validate(
            {
                "info": {
                    "name": "typed metadata test",
                },
                "type": "process",
            },
        )


def test_component_metadata_requires_type() -> None:
    with pytest.raises(ValidationError, match="type"):
        ComponentMetadata.model_validate(
            {
                "info": {
                    "id": "component-id",
                    "name": "typed metadata test",
                },
            },
        )


def test_component_metadata_requires_canonical_default_config_shape() -> None:
    with pytest.raises(ValidationError, match="defaultConfig.to_attr"):
        ComponentMetadata.model_validate(
            {
                "info": {
                    "id": "component-id",
                    "name": "typed metadata test",
                },
                "type": "standard",
                "defaultConfig": {
                    "to_attr": None,
                    "to_attr_type": "string",
                },
            },
        )


@pytest.mark.parametrize(
    "typed_metadata",
    [
        pytest.param(console_output_metadata, id="console-output"),
        pytest.param(split_string_metadata, id="split-string2"),
        pytest.param(to_string_metadata, id="to-string"),
        pytest.param(copy_metadata, id="copy"),
        pytest.param(load_balancer_metadata, id="load-balancer"),
        pytest.param(create_empty_raster_metadata, id="create-empty-raster"),
        pytest.param(filter_geoparquet_metadata, id="filter-geoparquet-by-raster"),
        pytest.param(relabel_geoparquet_metadata, id="relabel-geoparquet"),
        pytest.param(write_geoparquet_metadata, id="write-geoparquet"),
    ],
)
def test_component_metadata_round_trips_direct_json(typed_metadata: ComponentMetadata) -> None:
    metadata_json = typed_metadata.model_dump(mode="json", exclude_none=True)

    assert ComponentMetadata.model_validate(metadata_json).model_dump() == typed_metadata.model_dump()


def test_component_payload_excludes_category() -> None:
    payload = split_string_metadata.model_dump(
        mode="json",
        exclude={"category"},
        exclude_none=True,
    )

    assert "category" not in payload


def test_component_payload_excludes_config_model_but_keeps_derived_default_config() -> None:
    metadata = ComponentMetadata(
        info=meta.Info(id="inline-config", name="inline config"),
        type="process",
        config=_InlineConfig,
    )

    payload = metadata.model_dump(mode="json", exclude_none=True)

    assert "config" not in payload
    assert payload["defaultConfig"]["split_at"] == {
        "value": ",",
        "type": "string",
        "desc": "Split delimiter.",
    }
