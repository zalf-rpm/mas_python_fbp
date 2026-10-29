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
from typing import Any, Literal, override

import capnp
from mas.schema.common import common_capnp
from pydantic import Field
from zalfmas_common import common
from zalfmas_common.model import monica_io

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.components.json.update_json import read_attr_value, read_dict_value, split_into_parts
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
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether an IP holding no readable MONICA result is skipped or stops the process.",
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
        description=(
            "Write a MONICA CSV file. Substream transparent: bracket IPs are ignored, so they "
            "neither produce a file nor advance the running count."
        ),
    ),
    type="process",
    inPorts=[
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

    def results_of(self, in_ip: Any) -> dict[str, Any] | None:
        """The MONICA result this IP carries, or None if it carries none that can be read.

        Reading and parsing the payload is the only part of the per-IP work that can legitimately
        fail on bad input; a `try` around the whole body hid component faults behind it.
        """

        content_attr = common.get_fbp_attr(in_ip, self.config.from_attr)
        pointer = content_attr if content_attr else in_ip.content
        try:
            jstr = pointer.as_struct(common_capnp.StructuredText).value
        except capnp.KjException:
            return None
        try:
            result = json.loads(jstr)
        except (TypeError, ValueError):
            return None
        return result if isinstance(result, dict) else None

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        count = 0
        while self.in_ports["in"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                continue

            # Brackets are grouping, not data: writing one would produce a spurious file and
            # shift the running count used in filenames.
            if brackets.is_bracket(in_ip):
                continue

            attrs = {kv.key: kv.value for kv in in_ip.attributes}
            count += 1
            out_path_attr = common.get_fbp_attr(in_ip, self.config.out_path_attr)
            out_path = out_path_attr.as_text() if out_path_attr else self.config.path_to_out_dir

            result = self.results_of(in_ip)
            if result is None:
                message = f"{self.name}: no MONICA result could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

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
                parts = split_into_parts(expr, create_int_indizes=True)
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
            try:
                self.write_csv(filepath, result)
            except OSError:
                # the filesystem said no: a bad path, a full disk, a permission
                if self.config.on_error == "fail":
                    raise
                logger.exception("%s: could not write %s", self.name, filepath)
                continue
            except (KeyError, IndexError, TypeError, ValueError):
                # malformed caller data: monica_io indexes outputIds and results directly, so an
                # entry missing 'fromLayer' or a ragged results table surfaces from inside it
                if self.config.on_error == "fail":
                    raise
                logger.exception("%s: MONICA result in %s is malformed", self.name, filepath)
                filepath.unlink(missing_ok=True)
                continue

        logger.info("%s: process finished", self.name)

    def write_csv(self, filepath: Path, result: dict[str, Any]) -> None:
        """One CSV per MONICA result, a block per 'data' entry."""

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


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
