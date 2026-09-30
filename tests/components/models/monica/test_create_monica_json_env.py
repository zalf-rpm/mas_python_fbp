"""create_monica_json_env: narrowed error handling (plan LP5)."""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.models.monica.create_monica_json_env import METADATA, Component

SIM = {
    "include-file-base-path": "/tmp",
    "debug?": False,
    "climate.csv": "",
    "climate.csv-options": {"no-of-climate-file-header-lines": 1, "csv-separator": ","},
    "output": {"events": [], "obj-outputs?": True},
}
SITE = {
    "SiteParameters": {"Latitude": 52.0},
    "EnvironmentParameters": {"AtmosphericCO2": 380.0},
    "SoilMoistureParameters": {},
    "SoilTemperatureParameters": {},
    "SoilTransportParameters": {},
    "SoilOrganicParameters": {},
}
CROP = {"CropParameters": {}, "cropRotation": [{"worksteps": []}]}


def json_ip(payload):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(payload)))


def run(*, sim=None, crop=None, site=None, **settings):
    inputs: dict = {
        "sim": [json_ip(SIM if sim is None else sim), done_message()],
        "crop": [json_ip(CROP if crop is None else crop), done_message()],
        "site": [json_ip(SITE if site is None else site), done_message()],
    }
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(Component(METADATA), inputs=inputs, outputs=("out",))


def outputs(result):
    return [v.content.as_text() for v in result.output("out").values]


def test_it_emits_the_env_json() -> None:
    env = json.loads(outputs(run())[0])
    assert env["type"] == "Env"
    assert env["params"]["siteParameters"]["Latitude"] == 52.0


def test_templates_monica_rejects_emit_nothing_rather_than_the_string_null() -> None:
    """`create_env_json_from_json_config` answers None for templates that do not hold together.
    That None went straight to `json.dumps`, so the component emitted "null" as a valid env."""

    result = run(site={"SiteParameters": {}})  # missing every other section
    assert outputs(result) == []


def test_templates_monica_rejects_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="could not build an env"):
        run(site={"SiteParameters": {}}, on_error="fail")


def test_to_attr_puts_the_env_in_an_attribute_instead() -> None:
    result = run(to_attr="env")
    out = result.output("out").values[0]
    assert [entry.key for entry in out.attributes] == ["env"]


def test_it_stops_when_a_port_is_done() -> None:
    inputs: dict = {
        "sim": [done_message()],
        "crop": [done_message()],
        "site": [done_message()],
    }
    result = run_process_component(Component(METADATA), inputs=inputs, outputs=("out",))
    assert result.output("out").values == []


def test_a_fault_in_the_component_is_not_swallowed(monkeypatch) -> None:
    def boom(self, sim, crop, site):
        msg = "a fault inside the component"
        raise RuntimeError(msg)

    monkeypatch.setattr(Component, "env_template_for", boom)
    with pytest.raises(RuntimeError, match="a fault inside the component"):
        run()
