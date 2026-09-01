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
from __future__ import annotations

import logging
from pathlib import Path
from typing import override

from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class WriteFileConfig(process.ProcessConfig):
    id_attr: str = Field(
        "id",
        description="The attribute to get id for the filepattern from",
    )
    from_attr: str | None = Field(
        None,
        description="Instead of the IP content, get the content from that 'attr'.",
    )
    filepath_pattern: str = Field(
        "csv_{id}.csv",
        description="The pattern to use for the filename. Can contain {id} as placeholder for the id attribute.",
    )
    path_to_out_dir: str = Field(
        "path to output dir",
        description="The path to the output directory where the files will be written.",
    )
    append: bool = Field(
        False,
        description="If True, append to existing files instead of overwriting them.",
    )
    create_missing_dirs: bool = Field(
        False,
        description="If True, create missing directories in the output path.",
    )
    debug: bool = Field(
        False,
        description="If True, print debug information to the console.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="file",
        name="File",
    ),
    info=meta.Info(
        id="b3867019-5f42-4c59-9438-a49fe9452e6f",
        name="write file",
        description="Write input into a file.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
            desc="The input data to be written to a file.",
        ),
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    config=WriteFileConfig,
)


class WriteFile(process.Process[WriteFileConfig]):
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
        while True:
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            try:
                id_attr = common.get_fbp_attr(in_ip, self.config.id_attr)
                id_ = id_attr.as_text() if id_attr else str(count)
                content_attr = common.get_fbp_attr(in_ip, self.config.from_attr)
                content = content_attr.as_text() if content_attr else in_ip.content.as_text()

                filepath = Path(self.config.path_to_out_dir) / self.config.filepath_pattern.format(id=id_)
                if self.config.create_missing_dirs:
                    filepath.parent.mkdir(parents=True, exist_ok=True)

                with filepath.open("at" if self.config.append else "wt") as _:
                    _.write(content)
                    count += 1

                if self.config.debug:
                    logger.info("%s: wrote %s", self.name, filepath)

            except Exception:
                logger.exception("%s Exception", self.name)

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFile(METADATA), METADATA)


if __name__ == "__main__":
    main()
