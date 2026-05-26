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
from typing import override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    to_attr: str | None = Field(
        None,
        description="Set output into this attribute.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="models/monica",
        name="Models/MONICA",
    ),
    info=meta.Info(
        id="128af0c8-2614-4398-9043-ff3581958bd4",
        name="Create MONICA JSON env",
        description="Create MONICA JSON environment.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="sim",
            contentType="common.capnp:StructuredText[JSON] | Text (JSON)",
        ),
        meta.Port(
            name="crop",
            contentType="common.capnp:StructuredText[JSON] | Text (JSON)",
        ),
        meta.Port(
            name="site",
            contentType="common.capnp:StructuredText[JSON] | Text (JSON)",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
        ),
    ],
    config=Config,
)


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while self.in_ports["sim"] and self.in_ports["crop"] and self.in_ports["site"] and self.out_ports["out"]:
            try:
                sim = await p.read_dict_from_port_done(self.in_ports, "sim")
                crop = await p.read_dict_from_port_done(self.in_ports, "crop")
                site = await p.read_dict_from_port_done(self.in_ports, "site")

                if sim and crop and site:
                    env_template = monica_io.create_env_json_from_json_config(
                        {
                            "crop": crop,
                            "site": site,
                            "sim": sim,
                            "climate": "",
                        },
                    )

                    out_ip = fbp_capnp.IP.new_message()
                    if self.config.to_attr is not None and len(self.config.to_attr) > 0:
                        out_ip.attributes = [{"key": self.config.to_attr, "value": json.dumps(env_template)}]  # pyright: ignore
                    else:
                        out_ip.content = json.dumps(env_template)
                    if not await self.write_out("out", out_ip):
                        logger.info("%s: Could not send IP. Process finished.", self.name)

            except Exception as e:
                logger.exception("%s: Exception: %s", self.name, e)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
