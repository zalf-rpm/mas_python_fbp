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
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether a sim/crop/site trio MONICA rejects is skipped or stops the process.",
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

    def env_template_for(self, sim: dict, crop: dict, site: dict) -> dict[str, Any] | None:
        """The MONICA env for this trio, or None if MONICA will not build one from it.

        `create_env_json_from_json_config` answers None when the templates do not hold together,
        and raises KeyError when one lacks a section it needs. Both used to end up as output:
        the None was handed straight to `json.dumps`, so the component emitted the *string*
        "null" as though it were a valid env.
        """

        try:
            env_template = monica_io.create_env_json_from_json_config(
                {"crop": crop, "site": site, "sim": sim, "climate": ""},
            )
        except (KeyError, TypeError, ValueError):
            logger.exception("%s: MONICA rejected the sim/crop/site trio", self.name)
            return None
        return env_template if isinstance(env_template, dict) else None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        sent = 0
        while self.in_ports["sim"] and self.in_ports["crop"] and self.in_ports["site"] and self.out_ports["out"]:
            sim = await p.read_dict_from_port_done(self.in_ports, "sim")
            crop = await p.read_dict_from_port_done(self.in_ports, "crop")
            site = await p.read_dict_from_port_done(self.in_ports, "site")
            if not (sim and crop and site):
                continue

            env_template = self.env_template_for(sim, crop, site)
            if env_template is None:
                message = f"{self.name}: MONICA could not build an env from this sim/crop/site trio"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            out_ip = fbp_capnp.IP.new_message()
            if self.config.to_attr:
                out_ip.attributes = [{"key": self.config.to_attr, "value": json.dumps(env_template)}]  # pyright: ignore
            else:
                out_ip.content = json.dumps(env_template)
            if not await self.write_out("out", out_ip):
                logger.info("%s: could not send IP; stopping.", self.name)
                break
            sent += 1

        logger.info("%s process finished, sent %d env(s)", self.name, sent)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
