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
from typing import override

from mas.schema.common import common_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.json.update_json import read_attr_value, read_dict_value
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    types: dict[str, str] = Field(
        {"@object": "@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup"},
        description="Define the loadable type the attribute being referenced has.",
    )
    path_to_out_dir: str = Field(
        "out/",
        description="Use path_to_out_dir if no out_path_attr is available in metadata of IP.",
    )
    out_path_attr: str | None = Field(
        None,
        description="If out_path_attr is available, don't use path_to_out_dir.",
    )
    from_attr: str | None = Field(
        None,
        description="Get file content from attribute 'from_attr'.",
    )
    filepath_pattern: str = Field(
        "csv_{@object/id}.csv",
        description="""Replace {@id} to create the filename. @some_attr_name refers to an attribute.
        It's type has to be defined in the 'types' attribute. If no @ is in front of the name, it will be
        interpreted to be either the results "customId" stringified itself {customId} or if not 'customId' to be
        a subpath access in the 'customId' JSON object. If the {name} contains '/' it will be treated as
        a path in an object, where names will be keys in objects and numbers will be indizes in a list.""",
    )
    csv_delimiter: str = Field(
        ",",
        description="Like ','. Use this string as delimiter for csv output.",
    )


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
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="Receive MONICA JSON result.",
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

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        count = 0
        while self.in_ports["in"]:
            try:
                in_ip = await self.read_in("in")
                if in_ip is None:
                    self.in_ports["in"] = None
                    continue

                attrs = {kv.key: kv.value for kv in in_ip.attributes}
                count += 1
                out_path_attr = common.get_fbp_attr(in_ip, self.config.out_path_attr)
                out_path = out_path_attr.as_text() if out_path_attr else self.config.path_to_out_dir

                content_attr = common.get_fbp_attr(in_ip, self.config.from_attr)
                jstr = (
                    content_attr.as_struct(common_capnp.StructuredText).value
                    if content_attr
                    else in_ip.content.as_struct(common_capnp.StructuredText).value
                )
                result = json.loads(jstr)

                dir_ = Path(out_path)
                if not (dir_.is_dir() and dir_.exists()):
                    try:
                        dir_.mkdir(parents=True)
                    except OSError:
                        logger.exception("%s: Couldn't create dir: %s ! Exiting.", self.name, dir_)
                        return

                file_pattern = self.config.filepath_pattern
                file_name = ""
                while (i := file_pattern.find("{")) != -1 and (k := file_pattern.find("}", i + 1)) != -1 and k > i + 1:
                    file_name += file_pattern[:i]
                    expr = file_pattern[i + 1 : k]
                    parts = expr.split("/")
                    for j in range(len(parts)):
                        if parts[j].isdigit():
                            parts[j] = int(parts[j])
                    if len(parts) > 0 and not parts[0].startswith("@"):
                        attr_val, success = read_dict_value(result, parts)
                    elif len(parts) > 0:
                        attr_val, success = read_attr_value(self.config.types, attrs, parts)
                    else:
                        success = False
                    file_name += str(attr_val) if success else str(count)
                    file_pattern = file_pattern[k + 1 :]
                file_name += file_pattern

                filepath = dir_ / file_name
                with filepath.open("w") as _:
                    writer = csv.writer(_, delimiter=self.config.csv_delimiter)

                    for data_ in result.get("data", []):
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

                            if isinstance(results[0], dict):
                                for row in monica_io.write_output_obj(output_ids, results):
                                    writer.writerow(row)
                            else:
                                for row in monica_io.write_output(output_ids, results):
                                    writer.writerow(row)

                        writer.writerow([])

            except Exception:
                logger.exception("%s Exception", Path(__file__).name)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
