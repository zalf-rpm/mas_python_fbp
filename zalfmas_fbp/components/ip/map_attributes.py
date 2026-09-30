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
from typing import Any, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    rename: dict[str, str] = Field(
        default_factory=dict,
        description="Old name -> new name. Applied before 'keep' and 'drop', which see the new names.",
    )
    keep: list[str] = Field(
        default_factory=list,
        description="Keep only these attributes. Empty keeps all of them.",
    )
    drop: list[str] = Field(default_factory=list, description="Remove these attributes.")
    defaults: dict[str, Any] = Field(
        default_factory=dict,
        description="Attributes to add where the IP does not already have one of that name.",
    )
    set: dict[str, Any] = Field(
        default_factory=dict,
        description="Attributes to set, replacing any the IP already has.",
    )


METADATA = meta.Component(
    category=meta.Category(id="ip", name="IP (Flow packages)"),
    info=meta.Info(
        id="7e133b74-8024-46e9-8d40-4b8333d8b9a4",
        name="Map attributes",
        description=(
            "Rename, keep, drop, default and set attributes in one pass. Supersedes 'remove "
            "attributes', which only drops."
        ),
    ),
    type="process",
    inPorts=[meta.Port(name="in", contentType="AnyPointer", desc="IPs to map.", required=True)],
    outPorts=[meta.Port(name="out", contentType="AnyPointer", desc="The mapped IPs.", required=True)],
    config=Config,
)


class MapAttributes(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _mapped(self, in_ip: Any) -> dict[str, Any]:
        """The outgoing attributes: readers where they are carried over, values where they are new."""
        renamed: dict[str, Any] = {}
        for name, kv in brackets.attr_readers(in_ip).items():
            renamed[self.config.rename.get(name, name)] = kv

        keep, drop = set(self.config.keep), set(self.config.drop)
        mapped = {name: kv for name, kv in renamed.items() if (not keep or name in keep) and name not in drop}

        for name, value in self.config.defaults.items():
            if name not in mapped:
                mapped[name] = value
        mapped.update(self.config.set)
        return mapped

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        mapped_count = 0
        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("out", in_ip):
                    break
                continue

            out_ip = fbp_capnp.IP.new_message()
            out_ip.content = in_ip.content
            if in_ip.sysAttributes.contentType:
                out_ip.sysAttributes.contentType = in_ip.sysAttributes.contentType
            brackets.set_attrs(out_ip, self._mapped(in_ip))

            mapped_count += 1
            if not await self.write_out("out", out_ip):
                break

        logger.info("%s process finished, mapped %d IP(s)", self.name, mapped_count)


def main():
    process.run_process_from_metadata_and_cmd_args(MapAttributes(METADATA), METADATA)


if __name__ == "__main__":
    main()
