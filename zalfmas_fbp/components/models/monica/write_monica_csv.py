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
# This file is part of the util library used by models created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import json
import os

from zalfmas_capnp_schemas_with_stubs import fbp_capnp
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p

meta = {
    "category": {
        "id": "models/monica",
        "name": "Models/MONICA"
    },
    "component": {
        "info": {
            "id": "92e48886-2728-4a78-b53e-5cb0d4ac415a",
            "name": "Write MONICA CSV",
            "description": "Write a MONICA CSV file."
        },
        "type": "standard",
        "inPorts": [
            {
                "name": "conf",
                "contentType": "common.capnp:StructuredText[JSON | TOML]"
            }, {
                "name": "in",
                "contentType": "string (MONICA JSON result)",
                "desc": "Receive MONICA JSON result.",
            }
        ],
        "outPorts": [],
        "defaultConfig": {
            "path_to_out_dir": {
                "value": "out/",
                "type": "string",
                "desc": "Use path_to_out_dir if no out_path_attr is available in metadata of IP.",
            },
            "out_path_attr": {
                "value": "out_path",
                "type": "string",
                "desc": "If out_path_attr is available, don't use path_to_out_dir.",
            },
            "id_attr": {
                "value": "id",
                "type": "string",
                "desc": "Name of attribute which contains id to use for file name pattern.",
            },
            "from_attr": {
                "value": None,
                "type": "string",
                "desc": "Get file content from attribute 'from_attr'."
            },
            "filepath_pattern": {
                "value": "csv_{id}.csv",
                "type": "string",
                "desc": "Pattern with 'id' field (csv_{id}.csv)]. Write files name where id is replaced.",
            },
            "csv_delimiter": {
                "value": ",",
                "type": "string",
                "desc": "Like ','. Use this string as delimiter for csv output.",
            }
        }
    }
}


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"]
    )
    await p.update_config_from_port(config, ports["conf"])

    count = 0
    while ports["in"]:
        try:
            in_msg = ports["in"].read().wait()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            id_attr = common.get_fbp_attr(in_ip, config["id_attr"])
            count += 1
            id_ = id_attr.as_text() if id_attr else str(count)
            out_path_attr = common.get_fbp_attr(in_ip, config["out_path_attr"])
            out_path = (
                out_path_attr.as_text() if out_path_attr else config["path_to_out_dir"]
            )

            dir_ = out_path
            if os.path.isdir(dir_) and os.path.exists(dir_):
                pass
            else:
                try:
                    os.makedirs(dir_)
                except OSError:
                    print("c: Couldn't create dir:", dir_, "! Exiting.")
                    exit(1)

            filepath = os.path.join(dir_, config["file_pattern"].format(id=id_))
            with open(filepath, "w") as _:
                writer = csv.writer(_, delimiter=config["delimiter"])

                content_attr = common.get_fbp_attr(in_ip, config["from_attr"])
                jstr = (
                    content_attr.as_text() if content_attr else in_ip.content.as_text()
                )
                j = json.loads(jstr)

                for data_ in j.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace('"', "")])
                        for row in monica_io.write_output_header_rows(
                                output_ids,
                                include_header_row=True,
                                include_units_row=True,
                                include_time_agg=False,
                        ):
                            writer.writerow(row)

                        if len(results) > 0 and type(results[0]) == dict:
                            for row in monica_io.write_output_obj(output_ids, results):
                                writer.writerow(row)
                        else:
                            for row in monica_io.write_output(output_ids, results):
                                writer.writerow(row)

                    writer.writerow([])

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


def main():
    c.run_component_from_metadata(run_component, meta)


if __name__ == "__main__":
    main()
