from __future__ import annotations

import pytest
from pydantic import Field, ValidationError

from zalfmas_fbp.components.console.console_output import METADATA as console_output_metadata
from zalfmas_fbp.components.dakis.create_empty_raster import (
    METADATA as create_empty_raster_metadata,
)
from zalfmas_fbp.components.dakis.fetch_rbs_by_raster import (
    METADATA as fetch_rbs_metadata,
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
from zalfmas_fbp.components.string.split_string import METADATA as split_string_metadata
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
        pytest.param(fetch_rbs_metadata, id="fetch-rbs-by-raster"),
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


# --- port roles and runtime-owned ports (plan section 6.3, WP-1) ------------------------------


def _process_metadata(**kwargs) -> meta.ComponentMetadata:
    defaults = {
        "info": meta.Info(id="00000000-0000-4000-8000-000000000000", name="x"),
        "type": "process",
    }
    return meta.ComponentMetadata(**{**defaults, **kwargs})


def test_ports_default_to_the_data_role_and_are_optional() -> None:
    port = meta.Port(name="in")
    assert (port.role, port.required) == ("data", False)


@pytest.mark.parametrize(
    ("name", "role"),
    [("conf", "config"), ("log", "log"), ("err", "error"), ("rej", "reject")],
)
def test_reserved_names_get_their_role_without_having_to_declare_it(name, role) -> None:
    assert meta.Port(name=name).role == role


def test_a_reserved_name_may_not_claim_a_different_role() -> None:
    with pytest.raises(ValidationError, match="reserved for role"):
        meta.Port(name="conf", role="error")


@pytest.mark.parametrize("role", ["config", "log"])
def test_runtime_roles_may_not_be_claimed_by_another_port_name(role) -> None:
    with pytest.raises(ValidationError, match="is reserved for a port named"):
        meta.Port(name="whatever", role=role)


def test_a_process_component_gets_the_runtime_owned_conf_and_log_ports() -> None:
    component = _process_metadata(inPorts=[meta.Port(name="in")], outPorts=[meta.Port(name="out")])

    assert [(p.name, p.role) for p in component.inPorts] == [("in", "data"), ("conf", "config")]
    assert [(p.name, p.role) for p in component.outPorts] == [("out", "data"), ("log", "log")]


def test_a_component_still_declaring_conf_keeps_its_own_entry() -> None:
    """Migration: declaring it is redundant but must not produce a duplicate."""
    component = _process_metadata(inPorts=[meta.Port(name="conf", desc="mine"), meta.Port(name="in")])

    conf_ports = [p for p in component.inPorts if p.name == "conf"]
    assert len(conf_ports) == 1
    assert conf_ports[0].desc == "mine"
    assert conf_ports[0].role == "config"


def test_standard_components_get_no_runtime_owned_ports() -> None:
    """They are not Process based, so there is no runtime to own them."""
    component = meta.ComponentMetadata(
        info=meta.Info(id="00000000-0000-4000-8000-000000000001", name="legacy"),
        type="standard",
        inPorts=[meta.Port(name="in")],
        outPorts=[meta.Port(name="out")],
    )
    assert [p.name for p in component.inPorts] == ["in"]
    assert [p.name for p in component.outPorts] == ["out"]


def test_required_is_carried_through() -> None:
    component = _process_metadata(inPorts=[meta.Port(name="in", required=True)])
    assert [(p.name, p.required) for p in component.inPorts] == [("in", True), ("conf", False)]


def test_port_messages_report_role_and_requiredness_over_rpc() -> None:
    """What the flow editor and a runtime auto-wiring log ports actually see."""
    from zalfmas_fbp.components.ip.probe import METADATA as probe_metadata
    from zalfmas_fbp.components.ip.probe import Probe

    component = Probe(probe_metadata)
    in_ports = {m["name"]: m for m in component._port_runtime.in_port_messages()}
    out_ports = {m["name"]: m for m in component._port_runtime.out_port_messages()}

    assert in_ports["conf"]["role"] == "config"
    assert in_ports["in"]["role"] == "data"
    assert out_ports["log"]["role"] == "log"
    assert out_ports["out"]["role"] == "data"
    assert all("required" in m for m in (*in_ports.values(), *out_ports.values()))


def test_the_schema_accepts_the_reported_port_messages() -> None:
    """The dicts must round-trip through the Cap'n Proto Component.Port struct."""
    from mas.schema.fbp import fbp_capnp

    from zalfmas_fbp.components.ip.probe import METADATA as probe_metadata
    from zalfmas_fbp.components.ip.probe import Probe

    component = Probe(probe_metadata)
    for message in component._port_runtime.in_port_messages():
        port = fbp_capnp.Component.Port.new_message(**message)
        assert port.name == message["name"]
        assert str(port.role) == message["role"]
