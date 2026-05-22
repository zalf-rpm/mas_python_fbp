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

import logging
from pathlib import Path
from typing import override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class ReadFileConfig(process.ProcessConfig):
    to_attr: str | None = Field(
        None,
        description="store read file content into 'to_attr'",
    )
    file: str | None = Field(
        None,
        description="Path to file to read.",
    )
    lines_mode: bool = Field(
        True,
        description="Send single lines if true else send whole file content at once.",
    )
    skip_lines: int = Field(
        0,
        description="If lines mode is true, skip that many lines at the beginning of the file.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="file",
        name="File",
    ),
    info=meta.Info(
        id="7ba769ca-eba1-437c-b61a-bef27e24b1dc",
        name="read file",
        description="Read a file and send full string or lines downstream.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="Output either full file content or each line as as separate message.",
        ),
    ],
    config=ReadFileConfig,
)


class ReadFile(process.Process[ReadFileConfig]):
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

        if self.config.file is None or not self.out_ports["out"]:
            logger.info("%s: No filename supplied or out port closed. Process finished.", self.name)
            return

        try:
            skip_lines = self.config.skip_lines
            with Path(self.config.file).open() as file:
                if self.config.lines_mode:
                    for line in file:
                        if skip_lines > 0:
                            skip_lines -= 1
                            continue

                        out_ip = fbp_capnp.IP.new_message()
                        if self.config.to_attr is not None and len(self.config.to_attr) > 0:
                            out_ip.attributes = [{"key": self.config.to_attr, "value": line}]  # pyright: ignore
                        else:
                            out_ip.content = line
                        if not await self.write_out("out", out_ip):
                            logger.info("%s: Could not send IP. Process finished.", self.name)
                            return
                else:
                    file_content = file.read()
                    out_ip = fbp_capnp.IP.new_message()
                    if self.config.to_attr is not None and len(self.config.to_attr) > 0:
                        out_ip.attributes = [{"key": self.config.to_attr, "value": file_content}]
                    else:
                        out_ip.content = file_content
                    if not await self.write_out("out", out_ip):
                        logger.info("%s: Could not send IP. Process finished.", self.name)
                        return

        except capnp.KjException as e:
            logger.exception("%s: RPC Exception: %s", Path(__file__).name, e.description())

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(ReadFile(METADATA), METADATA)


if __name__ == "__main__":
    main()
