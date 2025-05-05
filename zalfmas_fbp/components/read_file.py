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
import capnp
import click
import io
import os
import sys
import tomli
import tomlkit as tk
from zalfmas_common import common
from zalfmas_fbp.run.ports import PortConnector
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await PortConnector.create_from_port_infos_reader(port_infos_reader_sr,
                                                              ins=["conf", "attr"], outs=["out"])

    if ports["conf"]:
        try:
            conf_msg = await ports["conf"].read()
            if conf_msg.which() != "done":
                conf_ip = conf_msg.value.as_struct(fbp_capnp.IP)
                conf_toml_str = conf_ip.content
                toml_config = tomli.loads(conf_toml_str)
                config.update(toml_config)
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    skip_lines = int(config["skip_lines"])
    if config["file"]:
        try:
            attr = None
            if ports["attr"]:
                attr_msg = await ports["attr"].read()
                if attr_msg.which() != "done":
                    attr_ip = attr_msg.value.as_struct(fbp_capnp.IP)
                    attr = attr_ip.content

            if ports["out"]:
                with open(config["file"]) as _:
                    if config["lines_mode"]:
                        for line in _.readlines():
                            if skip_lines > 0:
                                skip_lines -= 1
                                continue

                            out_ip = fbp_capnp.IP.new_message(content=line)
                            if attr and config["to_attr"]:
                                out_ip.attributes = [{"key": config["to_attr"], "value": attr}]
                            await ports["out"].write(value=out_ip)
                    else:
                        file_content = _.read()
                        out_ip = fbp_capnp.IP.new_message(content=file_content)
                        if attr and config["to_attr"]:
                            out_ip.attributes = [{"key": config["to_attr"], "value": attr}]
                        await ports["out"].write(value=out_ip)
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")

config = {
    "to_attr": "setup",
    "file": "/home/berg/Desktop/bahareh/run_cmd.txt",
    "lines_mode": (True, "send lines"),
    "skip_lines": (0, "skip no of lines lines in lines_mode=true"),
}
@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version='0.1.0')
@click.argument("port_infos_reader_sr", type=str, required=True)
@click.option("-w", "--write_toml_config", default="config.toml", show_default=True,
              is_eager=True, callback=lambda ctx, p, v: common.output_toml_config_click_callback(config, ctx, p, v),
              help="Create a config.toml template file in the current directory")
@click.option("-o", "--output_toml_config", is_flag=True, default=False,
              is_eager=True, callback=lambda ctx, p, v: common.output_toml_config_click_callback(config, ctx, p, v),
              help="Output TOML config at commandline")
def main(port_infos_reader_sr, **kwargs):
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))

if __name__ == '__main__':
    main()
