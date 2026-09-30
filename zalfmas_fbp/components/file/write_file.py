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
from typing import TYPE_CHECKING, override

import capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.common import brackets, templating, values
from zalfmas_fbp.run import metadata as meta

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

_NUMERIC_VALUE_VARIANTS = frozenset({"i8", "i16", "i32", "i64", "ui8", "ui16", "ui32", "ui64", "f32", "f64"})


class WriteFileConfig(process.ProcessConfig):
    from_attr: str | None = Field(
        None,
        description="Instead of the IP content, get the content from that 'attr'.",
    )
    filename_pattern: str = Field(
        "csv_{count}.csv",
        description="""The pattern to use for the filename. Can contain multiple placeholders. Use
        '{@attr_name}' to insert the value of the IP attribute 'attr_name', '{@attr/sub}' to reach into it,
        '{./a/b}' for a path into the content, and '{now:%Y-%m-%d}' for the time. A format spec after ':'
        works as in str.format, e.g. '{count:03d}'. Use '{count}' to insert the running count of received messages
        (starting at 0).""",
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
    attr_types: dict[str, str] = Field(
        default_factory=lambda: {values.ATTR_TYPE_WILDCARD: values.VALUE_TYPE},
        description=(
            "Cap'n Proto types for attributes written without a valueType, by attribute name, with "
            "'*' covering all of them. Defaults to treating them as common.capnp:Value, which is "
            "this library's convention - declaring the type is needed because reading a struct "
            "without one would mean guessing, and a wrong guess misreads silently rather than failing."
        ),
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
        description=(
            "Write input into a file. Substream transparent: bracket IPs are ignored, so they "
            "neither produce a file nor advance the '{count}' placeholder."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
            desc="The input data to be written to a file.",
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

        count = 0
        while True:
            in_ip = await self.read_in("in")
            if in_ip is None:
                break

            # Brackets are grouping, not data: writing one would produce a spurious file, and
            # counting it would shift the '{count}' of every file after it.
            if brackets.is_bracket(in_ip):
                continue

            try:
                content_attr = common.get_fbp_attr(in_ip, self.config.from_attr)
                try:
                    content = content_attr.as_text() if content_attr else in_ip.content.as_text()
                except capnp.KjException:
                    # Not text: fall back to a representation rather than failing to write at all.
                    content = str(content_attr) if content_attr else str(in_ip.content)

                filename = self._render_filename(in_ip, count)
                filepath = Path(self.config.path_to_out_dir) / filename
                if self.config.create_missing_dirs:
                    filepath.parent.mkdir(parents=True, exist_ok=True)

                with filepath.open("at" if self.config.append else "wt") as _:
                    _.write(content)

                if self.config.debug:
                    logger.info("%s: wrote %s", self.name, filepath)

            except Exception:
                logger.exception("%s Exception", self.name)
            finally:
                count += 1

        logger.info("%s: process finished", self.name)

    def _render_filename(self, ip: IPReader, count: int) -> str:
        """Render the configured pattern. Raises TemplateError, which run() turns into a skip."""
        return templating.render(
            self.config.filename_pattern,
            ip,
            count=count,
            attr_types=self.config.attr_types,
            missing="error",
        )


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFile(METADATA), METADATA)


if __name__ == "__main__":
    main()
