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

import logging
from typing import TYPE_CHECKING, Any, Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.soil import soil_capnp
from pydantic import Field, field_validator
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

PROFILE_TYPE = "soil.capnp:Profile"
PROPERTY_NAMES: frozenset[str] = frozenset(soil_capnp.PropertyName.schema.enumerants.keys())


def _known_properties(value: list[str]) -> list[str]:
    unknown = sorted(set(value) - PROPERTY_NAMES)
    if unknown:
        msg = f"unknown soil propert{'y' if len(unknown) == 1 else 'ies'} {unknown}; known are {sorted(PROPERTY_NAMES)}"
        raise ValueError(msg)
    return value


class Config(process.ProcessConfig):
    from_attr: str | None = Field(
        default=None,
        description="Read the coordinate from this attribute instead of the content.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Write the profile to this attribute instead of the IP's content.",
    )
    mandatory: list[str] = Field(
        default_factory=lambda: ["soilType", "organicCarbon", "rawDensity"],
        description="Soil properties the result must have, or the query fails.",
    )
    optional: list[str] = Field(
        default_factory=list,
        description="Soil properties to include when available.",
    )
    only_raw_data: bool = Field(
        default=False,
        description="Only return what the source physically holds, deriving nothing from it.",
    )
    emit: Literal["first", "all"] = Field(
        default="first",
        description=(
            "A coordinate can be covered by several profiles, each over part of the area. Send "
            "only the first, or one IP per profile."
        ),
    )
    wrap_in_substream: bool = Field(
        default=False,
        description="With emit='all', wrap each coordinate's profiles in a substream.",
    )
    on_no_profiles: Literal["skip", "fail"] = Field(
        default="skip",
        description="What to do when the service has no profile at a coordinate.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP holding no readable coordinate is skipped or stops the process.",
    )

    @field_validator("mandatory", "optional")
    @classmethod
    def _known(cls, value: list[str]) -> list[str]:
        return _known_properties(value)


METADATA = meta.Component(
    category=meta.Category(id="soil", name="Soil"),
    info=meta.Info(
        id="89da0cb9-2079-4245-aecc-068194bc1637",
        name="Use soil service",
        description=(
            "Ask a soil service for the profiles closest to each incoming coordinate. A coordinate "
            "may be covered by several profiles; 'emit' decides whether to send the first or all "
            "of them. Substream transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="latlon",
            contentType="geo.capnp:LatLonCoord",
            desc="The coordinate to get soil profiles at.",
            required=True,
        ),
        meta.Port(
            name="service",
            contentType="soil.capnp:Service | SturdyRef",
            desc="Capability or sturdy ref to the service. Read once, before the first coordinate.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=PROFILE_TYPE,
            desc="A capability to the soil profile at the given coordinate.",
            required=True,
        ),
    ],
    config=Config,
)


class UseSoilService(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    def coord_of(self, in_ip: IPReader) -> Any | None:
        """The incoming coordinate, from an attribute or the content."""

        if self.config.from_attr:
            kv = values.attr_reader(in_ip, self.config.from_attr)
            if kv is None:
                return None
            pointer = kv.value
        else:
            pointer = in_ip.content
        try:
            return pointer.as_struct(geo_capnp.LatLonCoord)
        except capnp.KjException:
            return None

    async def write_profile(self, profile: Any, in_ip: IPReader) -> bool:
        out_ip = fbp_capnp.IP.new_message()
        extra: dict[str, Any] = {}
        if self.config.to_attr:
            extra[self.config.to_attr] = brackets.Attr(profile, PROFILE_TYPE)
        else:
            out_ip.content = profile
            out_ip.sysAttributes.contentType = PROFILE_TYPE
        brackets.copy_attrs(in_ip, out_ip, extra=extra)
        self.sent += 1
        return await self.write_out("out", out_ip)

    async def service_from_port(self) -> Any | None:
        """The one soil service this component works against, read before the first coordinate."""

        service_ip = await self.read_in("service")
        if service_ip is None:
            self.in_ports["service"] = None
            logger.error("%s: the 'service' port closed before sending a service.", self.name)
            return None
        service, _ = await self.cast_cap_or_connect(service_ip.content, soil_capnp.Service)
        if service is None:
            logger.error("%s: no soil service could be received or connected to.", self.name)
        return service

    async def emit_profiles(self, profiles: Any, in_ip: IPReader) -> bool:
        """Send the wanted profiles. False means stop the process."""

        wanted = list(profiles) if self.config.emit == "all" else [profiles[0]]
        bracketed = self.config.emit == "all" and self.config.wrap_in_substream

        if bracketed and not await self.write_out("out", brackets.make_bracket("openBracket")):
            return False
        for profile in wanted:
            if not await self.write_profile(profile, in_ip):
                return False
        return not bracketed or await self.write_out("out", brackets.make_bracket("closeBracket"))

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        service = await self.service_from_port()
        if service is None:
            return

        while self.in_ports["latlon"] and self.out_ports["out"]:
            in_ip = await self.read_in("latlon")
            if in_ip is None:
                self.in_ports["latlon"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            coord = self.coord_of(in_ip)
            if coord is None:
                message = f"{self.name}: no coordinate could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            query = {
                "mandatory": self.config.mandatory,
                "optional": self.config.optional,
                "onlyRawData": self.config.only_raw_data,
            }
            # The result has to be awaited before its fields are read; awaiting `.profiles` on the
            # promise itself, as this used to, is not a pipelined read of a list.
            profiles = (await service.closestProfilesAt(coord, query)).profiles

            if len(profiles) == 0:
                message = f"{self.name}: no soil profile at ({coord.lat}, {coord.lon})"
                if self.config.on_no_profiles == "fail":
                    raise ValueError(message)
                logger.info(message)
                continue

            if not await self.emit_profiles(profiles, in_ip):
                break

        logger.info("%s process finished, sent %d profile(s)", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(UseSoilService(METADATA), METADATA)


if __name__ == "__main__":
    main()
