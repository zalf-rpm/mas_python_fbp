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

import csv
import os

import capnp
from zalfmas_capnp_schemas_with_stubs import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {"id": "file", "name": "File"},
    "component": {
        "info": {
            "id": "0e7507f8-97ae-4479-a608-4c1ebf37c4ba",
            "name": "read csv",
            "description": "Read a csv file and send content as string downstream.",
        },
        "type": "standard",
        "inPorts": [{"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"}],
        "outPorts": [
            {
                "name": "out",
                "contentType": "mas.schema.model.monica.sim_setup_capnp:Setup",
                "desc": "A single row from the CSV file sent as Setup struct.",
            }
        ],
        "defaultConfig": {
            "id_col": {"value": "id", "type": "string", "desc": "The column to be used to unique identify a row."},
            "col_to_field_names": {
                "value": {},
                "type": "object",
                "desc": "Map CSV column names to field names in the Cap'n Proto struct. E.g. {'col1': 'field1', 'col2': 'field2'}.",
            },
            "send_ids": {
                "value": [],
                "type": "list",
                "desc": "Send only these ids as messages downstream. E.g. [1,2,3]",
            },
            "file": {"value": "path to csv file", "type": "string", "desc": "The path to the CSV file to be read."},
            "struct_type": {
                "value": "mas.schema.model.monica.sim_setup_capnp:Setup",
                "type": "string",
                "desc": "The Cap'n Proto struct type to fill from a CSV row.",
            },
            "to_attr": {
                "value": None,
                "type": "string",
                "desc": "Instead of sending a row as IP content, send it in this attribute.",
            },
        },
    },
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr, ins=["conf"], outs=["out"])
    await p.update_config_from_port(config, ports["conf"])

    struct_type, _ = common.load_capnp_module(config["struct_type"])
    struct_fieldnames = struct_type.schema.fieldnames
    struct_fields = struct_type.schema.fields
    col_to_field_names = config["col_to_field_names"]
    id_col = col_to_field_names.get(config["id_col"], config["id_col"])
    send_ids = config["send_ids"] if config["send_ids"] is not None else None

    if ports["out"]:
        try:
            with open(config["file"]) as _:
                # determine seperator char
                dialect = csv.Sniffer().sniff(_.read(), delimiters=";,\t")
                _.seek(0)
                # read csv with seperator char
                reader = csv.reader(_, dialect)
                header_cols = next(reader)
                for row in reader:
                    val = struct_type.new_message()
                    for i, header_col in enumerate(header_cols):
                        # potentially map header column names to names of the structs field names
                        header_col = col_to_field_names.get(header_col, header_col)
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

                    if send_ids is None or (id_col in struct_fieldnames and val.__getattr__(id_col) in send_ids):
                        out_ip = fbp_capnp.IP.new_message()
                        if config["to_attr"]:
                            out_ip.attributes = [{"key": config["to_attr"], "value": val}]
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


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
