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
import csv
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    struct_type, _ = common.load_capnp_module(config["path_to_capnp_struct"])
    struct_fieldnames = struct_type.schema.fieldnames
    struct_fields = struct_type.schema.fields
    id_col = config["id_col"]
    send_ids = config["send_ids"] if config["send_ids"] is not None else None

    if ports["out"]:
        try:
            with open(config["file"]) as _:
                key_to_data = {}
                # determine seperator char
                dialect = csv.Sniffer().sniff(_.read(), delimiters=";,\t")
                _.seek(0)
                # read csv with seperator char
                reader = csv.reader(_, dialect)
                header_cols = next(reader)
                for row in reader:
                    val = struct_type.new_message()
                    for i, header_col in enumerate(header_cols):
                        if header_col not in struct_fieldnames:
                            continue
                        value = row[i]
                        fld_type = struct_fields[header_col].proto.slot.type.which()
                        try:
                            if fld_type == "bool":
                                val.__setattr__(header_col, value.lower() == "true")
                            elif fld_type == "text":
                                val.__setattr__(header_col, value)
                            elif fld_type in ["float32", "float64"]:
                                val.__setattr__(header_col, float(value))
                            elif fld_type in [
                                "int8",
                                "int16",
                                "int32",
                                "int64",
                                "uint8",
                                "uint16",
                                "uint32",
                                "uint64",
                            ]:
                                val.__setattr__(header_col, int(value))
                            elif fld_type == "enum":
                                if value in struct_fields[header_col].schema.enumerants:
                                    val.__setattr__(header_col, value)
                        except ValueError:
                            continue

                    if send_ids is None or (
                        id_col in struct_fieldnames
                        and val.__getattr__(id_col) in send_ids
                    ):
                        out_ip = fbp_capnp.IP.new_message()
                        if config["to_attr"]:
                            out_ip.attributes = [
                                {"key": config["to_attr"], "value": val}
                            ]
                        else:
                            out_ip.content = val
                        await ports["out"].write(value=out_ip)

        except capnp.KjException as e:
            print(
                f"{os.path.basename(__file__)}: {config['name']} RPC Exception:",
                e.description,
            )

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "id_col": "id",
    "send_ids": [],  # 1,2,3 -> [] = all
    "file": "sim_setups_bgr_flow.csv",
    "path_to_capnp_struct": "bgr.capnp:Setup",  # "bla.capnp:MyType",
    "to_attr": None,
    "opt:send_ids": "[[] | 1,2,3] -> [] = all",
    "opt:path_to_capnp_struct": "[string (capnp_file.capnp:MyType)]",
    "port:conf": "[TOML string] -> component configuration",
    "port:out": "[list[text | float | int]] -> output split list cast to cast_to type",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Read a CSV file into user structs per line"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
