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

import asyncio
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"]
    )
    await p.update_config_from_port(config, ports["conf"])

    count = 0
    while ports["in"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            id_attr = common.get_fbp_attr(in_ip, config["id_attr"])
            id = id_attr.as_text() if id_attr else str(count)
            content_attr = common.get_fbp_attr(in_ip, config["from_attr"])
            content = (
                content_attr.as_text() if content_attr else in_ip.content.as_text()
            )

            filepath = config["path_to_out_dir"] + config["filepath_pattern"].format(
                id=id
            )
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

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


"""
#id_attr = "id"
#from_attr = 
#filepath_pattern = "csv_{id}.csv"
#path_to_out_dir = "path to output dir"
#append = false
#debug = false
"""
default_config = {
    "id_attr": "id",
    "from_attr": None,
    "filepath_pattern": "csv_{id}.csv",
    "path_to_out_dir": "/home/berg/GitHub/mas-infrastructure/src/python/fbp/out/",
    "append": False,
    "debug": False,
    "opt:from_attr": "[name:string] -> get file content from attibute set in 'from_attr'",
    "opt:append": "[true | false] -> open file to be written in append mode or overwrite mode ",
    "opt:debug": "[true | false] -> if true output filepath to console",
    "opt:file": "[string] -> path to file to read",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string] -> write string to file",
}


def main():
    parser = c.create_default_fbp_component_args_parser("Write a text file")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
