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

import asyncio
import csv
import json
import os

import capnp
from zalfmas_capnp_schemas import fbp_capnp
from zalfmas_common import common
from zalfmas_common.model import monica_io

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


default_config = {
    "path_to_out_dir": "out/",
    "out_path_attr": "out_path",
    "id_attr": "id",
    "from_attr": None,
    "filepath_pattern": "csv_{id}.csv",
    "csv_delimiter": ",",
    "opt:from_attr": "[name:string] -> get file content from attibute set in 'from_attr'",
    "opt:path_to_out_dir": "[string (path)] -> use path_to_out_dir if no out_path_attr is available in metadata of IP",
    "opt:out_path_attr": "[string (path)] -> if out_path_attr is available, don't use path_to_out_dir",
    "opt:id_attr": "[string (name)] -> name of attribute which contains id to use for file name pattern",
    "opt:filepath_pattern": "[string pattern with 'id' field (csv_{id}.csv)] -> write files name where id is replaced",
    "opt:csv_delimiter": "[string (like ',')] -> use this string as delimiter for csv output",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string (MONICA JSON result)] -> receive MONICA JSON result",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Write a MONICA CSV output file"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
