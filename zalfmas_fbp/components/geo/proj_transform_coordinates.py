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
from pydantic import Field
from zalfmas_common import common, geo

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.components.geo import _coord_types as coord_types
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    from_name: str = Field(
        default="LatLon",
        description=(
            "Source CRS: one of LatLon, WGS84, GKx (x=2-5), UTMab (a=[1-60], b=[C-X]). Case does "
            "not matter. Ignored for IPs that declare their own content type."
        ),
    )
    to_name: str = Field(
        default="LatLon",
        description="Target CRS, same names as 'from_name'. Case does not matter.",
    )
    from_attr: str | None = Field(
        default=None,
        description="Read the input coordinate from this attribute instead of the content.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Write the result to this attribute instead of the content.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an IP whose coordinate cannot be read is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="geo", name="Geo"),
    info=meta.Info(
        id="b753df51-40f1-4778-ac47-82858c8ef80c",
        name="Proj transform coords",
        description=(
            "Transform coordinates between CRSs using the Proj library. Reads the source type from "
            "the IP's own content type when it has one, falling back to 'from_name'. Substream "
            "transparent."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
            desc="Coordinates to transform.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="geo.capnp:LatLonCoord | geo.capnp:UTMCoord | geo.capnp:GKCoord",
            desc="The transformed coordinates, tagged with the type they were built as.",
            required=True,
        ),
    ],
    config=Config,
)


class ProjTransformCoordinates(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def source_coord(self, in_ip: IPReader, configured_type: Any) -> Any | None:
        """The incoming coordinate, read through whichever type actually applies."""
        if self.config.from_attr:
            kv = values.attr_reader(in_ip, self.config.from_attr)
            if kv is None:
                return None
            declared = values.resolve_schema(kv.valueType) if kv._has("valueType") else None  # noqa: SLF001
            pointer = kv.value
        else:
            declared = values.resolve_schema(values.content_type_of(in_ip))
            pointer = in_ip.content

        schema = declared if declared is not None and getattr(declared, "node", None) is not None else configured_type
        if schema is None:
            return None
        try:
            return pointer.as_struct(schema)
        except capnp.KjException:
            return None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        configured_type = coord_types.struct_type_for(self.config.from_name)
        if configured_type is None:
            logger.warning(
                "%s: 'from_name' %r is not a known CRS; only IPs declaring their own type can be read.",
                self.name,
                self.config.from_name,
            )
        if coord_types.struct_instance_for(self.config.to_name) is None:
            logger.error("%s: 'to_name' %r is not a known CRS.", self.name, self.config.to_name)
            return

        transformed = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            from_coord = self.source_coord(in_ip, configured_type)
            if from_coord is None:
                message = f"{self.name}: could not read a coordinate from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            to_coord: Any = geo.transform_from_to_geo_coord(from_coord, self.config.to_name.lower())
            content_type = coord_types.content_type_of(to_coord)

            out_ip = fbp_capnp.IP.new_message()
            extra: dict[str, Any] = {}
            if self.config.to_attr:
                out_ip.content = in_ip.content
                if (incoming_type := values.content_type_of(in_ip)) is not None:
                    out_ip.sysAttributes.contentType = incoming_type
                extra[self.config.to_attr] = brackets.Attr(to_coord, content_type)
            else:
                out_ip.content = to_coord
                if content_type is not None:
                    out_ip.sysAttributes.contentType = content_type
            brackets.copy_attrs(in_ip, out_ip, extra=extra)

            transformed += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, transformed %d coordinate(s)", self.name, transformed)


def main():
    process.run_process_from_metadata_and_cmd_args(ProjTransformCoordinates(METADATA), METADATA)


if __name__ == "__main__":
    main()
