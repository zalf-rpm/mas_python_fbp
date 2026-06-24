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
import logging
from pathlib import Path
from typing import Any

from mas.schema.fbp import fbp_capnp
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p
from zalfmas_fbp.components.json.update_json import read_attr_value
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)

METADATA = meta.Component(
    category=meta.Category(
        id="models/monica",
        name="Models/MONICA",
    ),
    info=meta.Info(
        id="92e48886-2728-4a78-b53e-5cb0d4ac415a",
        name="Write MONICA CSV",
        description="Write a MONICA CSV file.",
    ),
    type="standard",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="string (MONICA JSON result)",
            desc="Receive MONICA JSON result.",
        ),
    ],
    defaultConfig={
        "path_to_out_dir": meta.ConfigEntry(
            value="out/",
            type="string",
            desc="Use path_to_out_dir if no out_path_attr is available in metadata of IP.",
        ),
        "out_path_attr": meta.ConfigEntry(
            value=None,
            type="string",
            desc="If out_path_attr is available, don't use path_to_out_dir.",
        ),
        "id_attr": meta.ConfigEntry(
            value="id",
            type="string",
            desc="Name of attribute which contains id to use for file name pattern.",
        ),
        "from_attr": meta.ConfigEntry(
            value=None,
            type="string",
            desc="Get file content from attribute 'from_attr'.",
        ),
        "filepath_pattern": meta.ConfigEntry(
            value="csv_{id}.csv",
            type="string",
            desc="Pattern with 'id' field (csv_{id}.csv)]. Write files name where id is replaced.",
        ),
        "csv_delimiter": meta.ConfigEntry(
            value=",",
            type="string",
            desc="Like ','. Use this string as delimiter for csv output.",
        ),
    },
)


async def run_component(port_infos_reader_sr: str, config: dict[str, Any]):
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
            attrs = {kv.key: kv.value for kv in in_ip.attributes}
            # id_attr = common.get_fbp_attr(in_ip, config["id_attr"])
            count += 1
            # id_ = id_attr.as_text() if id_attr else str(count)
            out_path_attr = common.get_fbp_attr(in_ip, config["out_path_attr"])
            out_path = out_path_attr.as_text() if out_path_attr else config["path_to_out_dir"]

            dir_ = Path(out_path)
            if dir_.is_dir() and dir_.exists():
                pass
            else:
                try:
                    dir_.mkdir(parents=True)
                except OSError:
                    logger.exception("c: Couldn't create dir: %s ! Exiting.", dir_)
                    exit(1)

            file_pattern = config["file_pattern"]
            file_name = ""
            while (i := file_pattern.find("{")) != -1 and (k := file_pattern.find("}", i + 1)) != -1 and k > i + 1:
                file_name += file_pattern[:i]
                expr = file_pattern[i + 1 : k]
                parts = expr.split("/")
                for j in range(len(parts)):
                    if parts[j].isdigit():
                        parts[j] = int(parts[j])
                attr_val, success = read_attr_value({}, attrs, parts)
                if success:
                    file_name += str(attr_val)
                else:
                    file_name += str(count)
                file_pattern = file_pattern[k + 1 :]

            filepath = dir_ / file_name  # config["file_pattern"].format(id=id_)
            with filepath.open("w") as _:
                writer = csv.writer(_, delimiter=config["delimiter"])

                content_attr = common.get_fbp_attr(in_ip, config["from_attr"])
                jstr = content_attr.as_text() if content_attr else in_ip.content.as_text()
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

                        if len(results) > 0 and isinstance(results[0], dict):
                            for row in monica_io.write_output_obj(output_ids, results):
                                writer.writerow(row)
                        else:
                            for row in monica_io.write_output(output_ids, results):
                                writer.writerow(row)

                    writer.writerow([])

        except Exception:
            logger.exception("%s Exception", Path(__file__).name)

    await pc.close_out_ports()
    logger.info("%s: process finished", Path(__file__).name)


def main():
    c.run_component_from_metadata(run_component, METADATA)


if __name__ == "__main__":
    main()
