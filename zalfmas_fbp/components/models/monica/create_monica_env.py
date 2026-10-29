#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
from __future__ import annotations

import copy
import json
import logging
import uuid
from pathlib import Path
from typing import Any, Literal, override

from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.grid import grid_capnp
from mas.schema.model import model_capnp
from mas.schema.model.monica import monica_management_capnp as mgmt_capnp
from mas.schema.model.monica import sim_setup_capnp
from mas.schema.soil import soil_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.ports as p
from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

ENV_TYPE = "model.capnp:Env"


class Config(process.ProcessConfig):
    """Each setting names where a value comes from: '@name' reads attribute 'name' off the
    incoming IP, anything else is used as the literal value. An empty setting skips that value.

    The names here are the ones the component actually looks up. They used to be declared with an
    '_attr' suffix while the code read the bare name, so none of them ever matched.
    """

    coord: str = Field(
        default="@latlon",
        description="Lat/lon coordinate of this simulation. Required.",
    )
    setup: str = Field(
        default="@setup",
        description="A model/monica/sim_setup.capnp:Setup driving the run. Required.",
    )
    climate: str = Field(
        default="@climate",
        description="A climate.capnp:TimeSeries capability, or a path to a MONICA climate CSV.",
    )
    soil: str = Field(
        default="@soil",
        description="A soil.capnp:Profile capability, or a JSON array of MONICA soil layers.",
    )
    ilr: str = Field(
        default="@ilr",
        description="Sowing/harvest dates as a monica_management.capnp:ILRDates.",
    )
    dgm: str = Field(
        default="@dgm",
        description="Height above sea level, as a grid.capnp:Grid.Value or a number.",
    )
    slope: str = Field(
        default="@slope",
        description="Slope in percent, as a grid.capnp:Grid.Value or a number.",
    )
    id: str = Field(
        default="@id",
        description="Id for this env. If the attribute is absent, a UUID4 is generated.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP missing a coordinate or setup is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="models/monica", name="Models/MONICA"),
    info=meta.Info(
        id="921bcda7-d83f-4190-8593-fce793dc9519",
        name="Create MONICA env",
        description=(
            "Build a MONICA env from sim/crop/site JSON templates named by an incoming Setup, "
            "overlaying the coordinate, elevation, slope, sowing/harvest dates, climate and soil "
            "of each IP. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="An IP carrying the run's values in its attributes. See the configuration.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=ENV_TYPE,
            desc="An Env ready to be sent to a MONICA instance.",
            required=True,
        ),
    ],
    config=Config,
)


class EnvTemplates:
    """Loads and caches the sim/crop/site template trio a Setup names.

    Each caller gets a deep copy. The old code handed out the cached dict itself and then mutated
    it per IP, so the second run against the same templates inherited the first run's elevation,
    slope, sowing dates and everything else.
    """

    def __init__(self):
        self._cache: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.loads: int = 0

    def get(self, sim: str, crop: str, site: str, crop_id: str) -> dict[str, Any]:
        key = (sim, crop, site, crop_id)
        if key not in self._cache:
            self._cache[key] = self._build(sim, crop, site, crop_id)
            self.loads += 1
        return copy.deepcopy(self._cache[key])

    @staticmethod
    def _build(sim: str, crop: str, site: str, crop_id: str) -> dict[str, Any]:
        with Path(sim).open() as handle:
            sim_json = json.load(handle)
        with Path(site).open() as handle:
            site_json = json.load(handle)
        with Path(crop).open() as handle:
            crop_json = json.load(handle)

        # the slot in the rotation naming which crop this run grows
        crop_json["cropRotation"][2] = crop_id

        env_template = monica_io.create_env_json_from_json_config(
            {"crop": crop_json, "site": site_json, "sim": sim_json, "climate": ""},
        )
        if env_template is None:
            msg = f"MONICA rejected the templates {sim!r}, {crop!r}, {site!r}"
            raise ValueError(msg)
        env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
        return env_template


def iso(day: Any) -> str:
    return f"{day.year:04d}-{day.month:02d}-{day.day:02d}"


def plain_value(value: Any) -> Any:
    """A JSON-serialisable value for something that was not the Cap'n Proto type asked for.

    It is either a literal straight from the config, or an attribute holding text rather than the
    expected struct or capability. A raw Cap'n Proto pointer would blow up in `json.dumps` later.
    """

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        return value.as_text()
    except Exception:  # noqa: BLE001 - any failure here just means "not usable as a plain value"
        return None


def numeric(value: Any) -> float | None:
    """`value` as a number, or None if it is not one."""

    plain = plain_value(value)
    if isinstance(plain, bool) or plain is None:
        return None
    if isinstance(plain, (int, float)):
        return float(plain)
    try:
        return float(plain)
    except (TypeError, ValueError):
        return None


def write_attributes(out_ip: Any, attrs: dict[str, Any], generated: dict[str, Any]) -> None:
    """Carry the incoming attributes over, writing the ones this component made itself as Values.

    Everything that arrived is passed through as the pointer it already is - re-encoding it would
    mean guessing its type, which Cap'n Proto cannot tell us (D14). Only values created here are
    written as a `common.Value` with its `valueType` set, which is the library's convention (D4).
    """

    if not attrs:
        return
    entries = out_ip.init("attributes", len(attrs))
    for entry, (key, value) in zip(entries, attrs.items(), strict=True):
        entry.key = key
        if key in generated:
            entry.value = values.value_from_python(generated[key])
            entry.valueType = values.VALUE_TYPE
        else:
            entry.value = value


def apply_ilr_dates(env_template: dict[str, Any], ilr: Any) -> None:
    """Overlay the sowing and harvest dates onto the template's first crop rotation."""

    worksteps = env_template["cropRotation"][0]["worksteps"]
    sowing = next((ws for ws in worksteps if ws["type"].endswith("Sowing")), None)
    harvest = next((ws for ws in worksteps if ws["type"].endswith("Harvest")), None)

    if sowing is not None:
        for field, key in (("sowing", "date"), ("earliestSowing", "earliest-date"), ("latestSowing", "latest-date")):
            if ilr._has(field):  # noqa: SLF001 - capnp's own API for "is this field set"
                sowing[key] = iso(getattr(ilr, field))
    if harvest is not None:
        for field, key in (("harvest", "date"), ("latestHarvest", "latest-date")):
            if ilr._has(field):  # noqa: SLF001
                harvest[key] = iso(getattr(ilr, field))


def apply_stage_temperature_sum(env_template: dict[str, Any], raw: str, name: str) -> None:
    """Replace the cultivar's stage temperature sums, if the given list is the right length."""

    try:
        stage_ts = [int(part) for part in raw.split("_")]
    except ValueError:
        logger.warning("%s: 'stageTemperatureSum' %r is not a list of integers; ignoring it.", name, raw)
        return

    cultivar = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]
    original = cultivar["StageTemperatureSum"][0]
    if len(stage_ts) != len(original):
        logger.warning(
            "%s: the provided StageTemperatureSum has %d entries, not %d; keeping the original.",
            name,
            len(stage_ts),
            len(original),
        )
        return
    cultivar["StageTemperatureSum"][0] = stage_ts


class CreateMonicaEnv(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.templates = EnvTemplates()
        self.sent: int = 0

    def apply_setup(self, env_template: dict[str, Any], setup: Any, coord: Any, attrs: dict[str, Any]) -> None:
        """Overlay everything the Setup and this IP say onto the template."""

        crop_params = env_template["params"]["userCropParameters"]
        site_params = env_template["params"]["siteParameters"]
        sim_params = env_template["params"]["simulationParameters"]
        env_params = env_template["params"]["userEnvironmentParameters"]

        crop_params["__enable_vernalisation_factor_fix__"] = setup.useVernalisationFix
        crop_params["__enable_T_response_leaf_expansion__"] = setup.leafExtensionModifier

        if self.config.ilr:
            ilr, is_capnp = p.get_attr_val(self.config.ilr, attrs, as_struct=mgmt_capnp.ILRDates, remove=True)
            if is_capnp:
                apply_ilr_dates(env_template, ilr)

        if setup.elevation and self.config.dgm:
            height_nn, is_capnp = p.get_attr_val(self.config.dgm, attrs, as_struct=grid_capnp.Grid.Value, remove=True)
            if is_capnp:
                site_params["heightNN"] = height_nn.f
            elif (value := numeric(height_nn)) is not None:
                site_params["heightNN"] = value

        if setup.slope and self.config.slope:
            slope, is_capnp = p.get_attr_val(self.config.slope, attrs, as_struct=grid_capnp.Grid.Value, remove=True)
            if is_capnp:
                site_params["slope"] = slope.f / 100.0
            elif (value := numeric(slope)) is not None:
                site_params["slope"] = value / 100.0

        if setup.latitude:
            site_params["Latitude"] = coord.lat
        if setup.co2 > 0:
            env_params["AtmosphericCO2"] = setup.co2
        if setup.o3 > 0:
            env_params["AtmosphericO3"] = setup.o3
        if setup.fieldConditionModifier:
            species = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]
            species["FieldConditionModifier"] = setup.fieldConditionModifier
        if len(setup.stageTemperatureSum) > 0:
            apply_stage_temperature_sum(env_template, setup.stageTemperatureSum, self.name)

        sim_params["UseNMinMineralFertilisingMethod"] = setup.fertilization
        sim_params["UseAutomaticIrrigation"] = setup.irrigation
        sim_params["NitrogenResponseOn"] = setup.nitrogenResponseOn
        sim_params["WaterDeficitResponseOn"] = setup.waterDeficitResponseOn
        sim_params["EmergenceMoistureControlOn"] = setup.emergenceMoistureControlOn
        sim_params["EmergenceFloodingControlOn"] = setup.emergenceFloodingControlOn

    def build_env(self, env_template: dict[str, Any], attrs: dict[str, Any]) -> Any:
        """The Env message, with climate and soil either attached as capabilities or inlined."""

        capnp_env = model_capnp.Env.new_message()

        if self.config.climate:
            timeseries, is_capnp = p.get_attr_val(
                self.config.climate, attrs, as_interface=climate_capnp.TimeSeries, remove=True
            )
            if is_capnp:
                capnp_env.timeSeries = timeseries
            elif path := plain_value(timeseries):
                env_template["pathToClimateCSV"] = path

        if self.config.soil:
            soil_profile, is_capnp = p.get_attr_val(
                self.config.soil, attrs, as_interface=soil_capnp.Profile, remove=True
            )
            if is_capnp:
                capnp_env.soilProfile = soil_profile
            elif layers := plain_value(soil_profile):
                # documented as a JSON array of MONICA layers; MONICA wants the array, not its text
                try:
                    layers = json.loads(layers)
                except (TypeError, ValueError):
                    pass
                env_template["params"]["siteParameters"]["SoilProfileParameters"] = layers

        capnp_env.rest = common_capnp.StructuredText.new_message(value=json.dumps(env_template), type="json")
        return capnp_env

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            attrs: dict[str, Any] = {kv.key: kv.value for kv in in_ip.attributes}

            coord, coord_is_capnp = p.get_attr_val(
                self.config.coord, attrs, as_struct=geo_capnp.LatLonCoord, remove=True
            )
            setup, setup_is_capnp = p.get_attr_val(
                self.config.setup, attrs, as_struct=sim_setup_capnp.Setup, remove=True
            )
            if not coord_is_capnp or not setup_is_capnp:
                missing = [name for name, ok in (("coord", coord_is_capnp), ("setup", setup_is_capnp)) if not ok]
                message = f"{self.name}: this IP has no {' and no '.join(missing)}"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            env_template = self.templates.get(setup.simJson, setup.cropJson, setup.siteJson, setup.cropId)
            self.apply_setup(env_template, setup, coord, attrs)

            id_, id_is_attr = p.get_attr_val(self.config.id, attrs, as_text=True, remove=False)
            generated: dict[str, Any] = {}
            if not id_is_attr:
                id_ = str(uuid.uuid4())
                attrs["id"] = generated["id"] = id_

            env_template["customId"] = {
                "setup_id": setup.runId,
                "id": id_,
                "crop_id": setup.cropId,
                "lat": coord.lat,
                "lon": coord.lon,
            }

            out_ip = fbp_capnp.IP.new_message()
            out_ip.content = self.build_env(env_template, attrs)
            out_ip.sysAttributes.contentType = ENV_TYPE
            write_attributes(out_ip, attrs, generated)

            self.sent += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, sent %d env(s)", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(CreateMonicaEnv(METADATA), METADATA)


if __name__ == "__main__":
    main()
