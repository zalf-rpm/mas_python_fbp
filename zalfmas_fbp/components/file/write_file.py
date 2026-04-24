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

import os

import capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "file", "name": "File"},
    "component": {
        "info": {
            "id": "b3867019-5f42-4c59-9438-a49fe9452e6f",
            "name": "write file",
            "description": "Write input into a file.",
        },
        "type": "standard",
        "inPorts": [
            {"name": "in", "contentType": "Text", "desc": "The input data to be written to a file."},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [],
        "defaultConfig": {
            "id_attr": {"value": "id", "type": "string", "desc": "The attribute to get id for the filepattern from"},
            "from_attr": {
                "value": None,
                "type": "string",
                "desc": "Instead of the IP content, get the content from that 'attr'.",
            },
            "filepath_pattern": {
                "value": "csv_{id}.csv",
                "type": "string",
                "desc": "The pattern to use for the filename. Can contain {id} as placeholder for the id attribute.",
            },
            "path_to_out_dir": {
                "value": "path to output dir",
                "type": "string",
                "desc": "The path to the output directory where the files will be written.",
            },
            "append": {
                "value": False,
                "type": "bool",
                "desc": "If True, append to existing files instead of overwriting them.",
            },
            "debug": {"value": False, "type": "bool", "desc": "If True, print debug information to the console."},
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf", "in"])
    await p.update_config_from_port(config, pc.in_ports["conf"])

    count = 0
    while pc.in_ports["in"]:
        try:
            in_msg = await pc.in_ports["in"].read()
            if in_msg.which() == "done":
                pc.in_ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            id_attr = common.get_fbp_attr(in_ip, config["id_attr"])
            id = id_attr.as_text() if id_attr else str(count)
            content_attr = common.get_fbp_attr(in_ip, config["from_attr"])
            content = content_attr.as_text() if content_attr else in_ip.content.as_text()

            filepath = config["path_to_out_dir"] + config["filepath_pattern"].format(id=id)
            with open(filepath, "at" if config["append"] else "wt") as _:
                _.write(content)
                count += 1

            if config["debug"]:
                print("wrote", filepath)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )
            if e.type in ["DISCONNECTED"]:
                break

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await pc.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
