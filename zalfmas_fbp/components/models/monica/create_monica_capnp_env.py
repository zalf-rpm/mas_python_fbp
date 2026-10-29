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

import json
import logging
from typing import Any, Literal, override

import capnp
from mas.schema.climate import climate_capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.model import model_capnp
from mas.schema.soil import soil_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    from_attr: str | None = Field(
        None,
        description="Instead of the message content, read the MONICA JSON env from this attribute.",
    )
    to_attr: str | None = Field(
        None,
        description="Instead of sending the ready prepared MONICA Cap'n Proto env via the message content, send it via this attribute.",
    )
    timeseries_attr: str = Field(
        "@timeseries",
        description="Either a capability to a time series (via @ out of attribute) or the path to a MONICA compatible climate CSV file from config value.",
    )
    remove_timeseries_attr: bool = Field(
        True,
        description="Remove climate attribute from message after use.",
    )
    soil_attr: str = Field(
        "@soil",
        description="Either a capability to a soil profile (via @ out of attribute) or a string (JSON array) containing a MONICA soil profile description from config value.",
    )
    remove_soil_attr: bool = Field(
        True,
        description="Remove soil attribute from message after use.",
    )
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether an IP holding no readable MONICA JSON env is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="models/monica",
        name="Models/MONICA",
    ),
    info=meta.Info(
        id="e58b7ff4-3c76-4ea2-9873-09d6923e5c75",
        name="Create model.capnp:Env with MONICA payload",
        description="Create model.capnp:Env with MONICA JSON env payload.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="timeseries",
            contentType="@0xa7769f40fe6e6de8 = climate/climate.capnp:TimeSeries | @0xed6c098b67cad454 = common/common.capnp:StructuredText[SturdyRef | CSV]",
            desc="Climate data for MONICA simulation, either as a TimeSeries capability, a sturdy ref to a TimeSeries or a path to a CSV file.",
        ),
        meta.Port(
            name="soil",
            contentType="@0xff67c2a593419c29 = soil/soil.capnp:Profile | @0xed6c098b67cad454 = common/common.capnp:StructuredText[SturdyRef | JSON]",
            desc="Soil profile data for MONICA simulation, either as a Profile capability, a sturdy ref to a Profile or an JSON array of soil layers.",
        ),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="MONICA env json. Supports substreams on 'in' and will simply forward bracket IPs downstream.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="@0xb7fc866ef1127f7c = model/model.capnp:Env",
            desc="An Env structure with possible attached climate/soil capabilities ready to be sent to a MONICA Cap'n Proto service or component.",
        ),
    ],
    config=Config,
)


def set_soil_layers(json_env: dict[str, Any], layers: Any) -> None:
    """Put the soil layers into the env, creating the nesting rather than assuming it is there."""

    json_env.setdefault("params", {}).setdefault("siteParameters", {})["SoilProfileParameters"] = layers


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def json_env_of(self, in_ip: Any, in_attrs: dict[str, Any]) -> dict[str, Any] | None:
        """The MONICA JSON env this IP carries, or None if it carries none that can be read.

        Reading and parsing the payload is the only step here that can legitimately fail on bad
        input, so it is the only one guarded. Anything else going wrong is a fault in the
        component, and used to be swallowed by a `try` around the whole per-IP body - which makes
        a component that fails on every IP look exactly like one with no input at all.
        """

        if self.config.from_attr:
            json_env_str, is_capnp = p.get_attr_val(self.config.from_attr, in_attrs, remove=True)
            if not is_capnp:
                return None
            try:
                json_env_str = json_env_str.as_text()
            except (capnp.KjException, AttributeError):
                return None
        else:
            try:
                json_env_str = in_ip.content.as_text()
            except capnp.KjException:
                return None

        try:
            json_env = json.loads(json_env_str)
        except (TypeError, ValueError):
            return None
        return json_env if isinstance(json_env, dict) else None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        timeseries = None
        timeseries_path = None
        soil_profile = None
        soil_profile_json_array = None
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                break
            # simply forward substream open and close brackets
            elif in_ip.type == "openBracket":
                if not await self.write_out("out", in_ip):
                    logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
                    break
                continue
            elif in_ip.type == "closeBracket":
                if not await self.write_out("out", in_ip):
                    logger.info("%s: error on sending on 'out' port. Process finished.", self.name)
                    break
                continue

            # readers for what arrived, plus whatever this component builds and puts back in
            in_attrs: dict[str, Any] = {kv.key: kv.value for kv in in_ip.attributes}

            json_env = self.json_env_of(in_ip, in_attrs)
            if json_env is None:
                message = f"{self.name}: no MONICA JSON env could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            capnp_env = model_capnp.Env.new_message()

            # Handle climate data
            timeseries_set = False
            # try to get timeseries from climate port
            if self.in_ports["timeseries"]:
                if (timeseries_ip := await self.read_in("timeseries")) is None:
                    self.in_ports["timeseries"] = None
                else:
                    timeseries, st_timeseries_path = await self.cast_cap_or_connect(
                        timeseries_ip.content, climate_capnp.TimeSeries
                    )
                    if timeseries:
                        capnp_env.timeSeries = timeseries
                        timeseries_set = True
                    elif st_timeseries_path and st_timeseries_path.type == "unstructured":
                        json_env["pathToClimateCSV"] = timeseries_path = st_timeseries_path.value
                        timeseries_set = True

            # try to set timeseries data from attributes
            # but only for this out IP
            if not timeseries_set:
                timeseries_val, is_capnp = p.get_attr_val(
                    self.config.timeseries_attr,
                    in_attrs,
                    remove=self.config.remove_timeseries_attr,
                )
                if is_capnp:
                    attr_timeseries, st_timeseries_path = await self.cast_cap_or_connect(
                        timeseries_val, climate_capnp.TimeSeries
                    )
                    if attr_timeseries:
                        capnp_env.timeSeries = attr_timeseries
                    elif st_timeseries_path and st_timeseries_path.type == "unstructured":
                        json_env["pathToClimateCSV"] = st_timeseries_path.value

            # if we have a previously used timeseries from the timeseries port, use these
            elif timeseries is not None:
                capnp_env.timeSeries = timeseries
            # if we have a previously used timeseries path from the timeseries port, use these
            elif timeseries_path is not None:
                json_env["pathToClimateCSV"] = timeseries_path

            # Handle soil data
            soil_set = False
            if self.in_ports["soil"]:
                if (soil_ip := await self.read_in("soil")) is None:
                    self.in_ports["soil"] = None
                else:
                    soil_profile, soil_st = await self.cast_cap_or_connect(soil_ip.content, soil_capnp.Profile)
                    if soil_profile:
                        capnp_env.soilProfile = soil_profile
                        soil_set = True
                    elif soil_st and soil_st.type == "json":
                        soil_profile_json_array = json.loads(soil_st.value)
                        set_soil_layers(json_env, soil_profile_json_array)
                        soil_set = True

            # try to set soil profile from attribute
            # set attribute profile only for outgoing IP
            if not soil_set:
                soil_val, is_capnp = p.get_attr_val(
                    self.config.soil_attr,
                    in_attrs,
                    remove=self.config.remove_soil_attr,
                )
                if is_capnp:
                    attr_soil_profile, soil_st = await self.cast_cap_or_connect(soil_val, soil_capnp.Profile)
                    if attr_soil_profile:
                        capnp_env.soilProfile = attr_soil_profile
                    elif soil_st and soil_st.type == "json":
                        set_soil_layers(json_env, json.loads(soil_st.value))

            # if we have a previously used soil_profile from the soil port, use these
            elif soil_profile is not None:
                capnp_env.soilProfile = soil_profile
            # if we have a previously used timeseries path from the timeseries port, use these
            elif soil_profile_json_array is not None:
                set_soil_layers(json_env, soil_profile_json_array)

            capnp_env.rest = common_capnp.StructuredText.new_message(
                value=json.dumps(json_env),
                type="json",
            )

            out_ip = fbp_capnp.IP.new_message()
            if self.config.to_attr is not None and len(self.config.to_attr) > 0:
                in_attrs[self.config.to_attr] = capnp_env
            else:
                out_ip.content = capnp_env

            out_ip.attributes = list([{"key": k, "value": v} for k, v in in_attrs.items()])  # pyright: ignore
            if not await self.write_out("out", out_ip):
                logger.info("%s: Could not send IP.", self.name)
                break

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
