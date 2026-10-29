"""A config null is a value, not a request to unset (see base_components_plan.md section 7.9).

Value has no null variant, so a null is carried by leaving ConfigEntry.val unset - the same
convention Pair.snd already uses for nulls nested inside a config dict.
"""

from __future__ import annotations

import asyncio
import json
from typing import cast

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field, ValidationError

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process


class Config(process.ProcessConfig):
    nullable: str | None = Field(default="preset")
    required: str = Field(default="always")


METADATA = meta.Component(
    info=meta.Info(id="7a1a2b3c-0000-4000-8000-000000000098", name="null probe"),
    type="process",
    inPorts=[meta.Port(name="in")],
    outPorts=[meta.Port(name="out")],
    config=Config,
)


def component():
    return process.Process[Config](METADATA)


def conf_ip(**settings):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings)))


# --- the funnel -------------------------------------------------------------------------------


def test_null_sets_null_on_a_nullable_field() -> None:
    c = component()
    assert c.config.nullable == "preset"
    c.apply_config_values({"nullable": None})
    assert c.config.nullable is None


def test_null_on_a_non_nullable_field_is_rejected_by_the_model() -> None:
    """The contract is the model's to enforce; it could not do so while the key was removed first."""
    c = component()
    with pytest.raises(ValidationError):
        c.apply_config_values({"required": None})
    assert c.config.required == "always"


def test_setting_one_key_still_validates_the_whole_config() -> None:
    c = component()
    c.apply_config_values({"nullable": "a"})
    with pytest.raises(ValidationError):
        c.apply_config_values({"required": None})
    assert c.config.nullable == "a"


def test_a_null_default_is_applied_like_any_other() -> None:
    class WithNullDefault(process.ProcessConfig):
        thing: str | None = Field(default=None)

    metadata = meta.Component(
        info=meta.Info(id="7a1a2b3c-0000-4000-8000-000000000099", name="x"),
        type="process",
        config=WithNullDefault,
    )
    assert process.Process[WithNullDefault](metadata).config.thing is None


# --- over the conf port -------------------------------------------------------------------------


def test_a_null_arriving_on_the_conf_port_is_applied() -> None:
    c = component()
    run_process_component(
        c,
        inputs={"conf": [conf_ip(nullable=None), done_message()], "in": [done_message()]},
        outputs=(),
    )
    assert c.config.nullable is None


def test_an_invalid_null_on_the_conf_port_keeps_the_previous_config() -> None:
    c = component()
    run_process_component(
        c,
        inputs={"conf": [conf_ip(required=None), done_message()], "in": [done_message()]},
        outputs=(),
    )
    assert c.config.required == "always"


# --- over the RPC ---------------------------------------------------------------------------------


def test_set_config_entry_reads_an_unset_val_as_null() -> None:
    c = component()
    request = fbp_capnp.Process.ConfigEntry.new_message(name="nullable")

    class Ctx:
        params = request.as_reader()

    asyncio.run(c.setConfigEntry("nullable", request.as_reader().val, cast("object", Ctx)))
    assert c.config.nullable is None


def test_set_config_entry_still_reads_a_present_val() -> None:
    c = component()
    request = fbp_capnp.Process.ConfigEntry.new_message(
        name="nullable",
        val=common_capnp.Value.new_message(t="given"),
    )

    class Ctx:
        params = request.as_reader()

    asyncio.run(c.setConfigEntry("nullable", request.as_reader().val, cast("object", Ctx)))
    assert c.config.nullable == "given"


def test_config_entries_reports_a_null_by_leaving_val_unset() -> None:
    c = component()
    c.apply_config_values({"nullable": None})

    entries = {e.name: e for e in asyncio.run(c.configEntries(cast("object", None)))}
    assert entries["nullable"].as_reader()._has("val") is False
    assert entries["required"].as_reader()._has("val") is True


def test_config_entries_round_trips_a_null_back_into_a_component() -> None:
    """What an incremental UI needs: read the live config, change one field, send it back."""
    source = component()
    source.apply_config_values({"nullable": None})
    entries = asyncio.run(source.configEntries(cast("object", None)))

    target = component()
    for entry in entries:
        reader = entry.as_reader()

        class Ctx:
            params = reader

        asyncio.run(target.setConfigEntry(reader.name, reader.val, cast("object", Ctx)))

    assert target.config.nullable is None
    assert target.config.required == "always"
