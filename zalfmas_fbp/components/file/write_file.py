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
import string
from pathlib import Path
from typing import TYPE_CHECKING, override

import capnp
from mas.schema.common import common_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
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
        '{@attr_name}' to insert the value of the IP attribute 'attr_name' (the '@' is only a marker and is
        stripped before the attribute lookup). Use '{count}' to insert the running count of received messages
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
                content_attr = common.get_fbp_attr(in_ip, self.config.from_attr)
                content = content_attr.as_text() if content_attr else in_ip.content.as_text()

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
        pattern = self.config.filename_pattern
        values: dict[str, object] = {}
        for _literal_text, field_name, _format_spec, _conversion in string.Formatter().parse(pattern):
            if field_name is None:
                continue
            base_name = field_name.split(".")[0].split("[")[0]
            if base_name in values:
                continue

            if base_name == "count":
                values[base_name] = count
            elif base_name.startswith("@"):
                values[base_name] = self._attr_as_str(ip, base_name.removeprefix("@"))
            else:
                msg = (
                    f"{self.name}: filename_pattern references unknown placeholder "
                    f"'{{{base_name}}}'; use '{{@attr_name}}' or '{{count}}'"
                )
                raise ValueError(msg)

        return pattern.format(**values)

    def _attr_as_str(self, ip: IPReader, attr_name: str) -> str:
        value = common.get_fbp_attr(ip, attr_name)
        if value is None:
            msg = f"{self.name}: IP is missing attribute '{attr_name}' referenced in filename_pattern"
            raise ValueError(msg)

        try:
            return value.as_text()
        except (capnp.KjException, TypeError, AttributeError):
            pass

        try:
            common_value = value.as_struct(common_capnp.Value)
            which = common_value.which()
            if which in _NUMERIC_VALUE_VARIANTS or which == "t":
                return str(getattr(common_value, which))
        except (capnp.KjException, TypeError, AttributeError):
            pass

        return str(value)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFile(METADATA), METADATA)


if __name__ == "__main__":
    main()
