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
import os

from mas.schema.fbp import fbp_capnp
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

logger = logging.getLogger(__name__)

meta = {
    "category": {"id": "models/monica", "name": "Models/MONICA"},
    "component": {
        "info": {
            "id": "128af0c8-2614-4398-9043-ff3581958bd4",
            "name": "Create MONICA JSON env",
            "description": "Create MONICA JSON environment.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
            {"name": "sim", "contentType": "common.capnp:StructuredText[JSON] | Text (JSON)"},
            {"name": "crop", "contentType": "common.capnp:StructuredText[JSON] | Text (JSON)"},
            {"name": "site", "contentType": "common.capnp:StructuredText[JSON] | Text (JSON)"},
        ],
        "outPorts": [{"name": "out", "contentType": "Text (JSON)"}],
        "defaultConfig": {"to_attr": {"value": None, "type": "Text (JSON)", "desc": "Set output into this attribute."}},
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "sim", "crop", "site"], outs=["out"]
    )
    await p.update_config_from_port(config, pc.in_ports["conf"])

    while pc.in_ports["sim"] and pc.in_ports["crop"] and pc.in_ports["site"] and pc.out_ports["out"]:
        try:
            sim = await p.read_dict_from_port_done(pc, "sim")
            crop = await p.read_dict_from_port_done(pc, "crop")
            site = await p.read_dict_from_port_done(pc, "site")

            if sim and crop and site:
                env_template = monica_io.create_env_json_from_json_config(
                    {
                        "crop": crop,
                        "site": site,
                        "sim": sim,
                        "climate": "",
                    }
                )

                out_ip = fbp_capnp.IP.new_message()
                to_attr = config.get("to_attr", None)
                if to_attr and len(to_attr.as_text()) > 0:
                    out_ip.attributes = [{"key": to_attr.as_text(), "value": json.dumps(env_template)}]
                else:
                    out_ip.content = json.dumps(env_template)
                await pc.out_ports["out"].write(value=out_ip)

        except Exception:
            logger.exception("%s Exception", os.path.basename(__file__))

    await pc.close_out_ports()
    logger.info("%s: process finished", os.path.basename(__file__))


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
