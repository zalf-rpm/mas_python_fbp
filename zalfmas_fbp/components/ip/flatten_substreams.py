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
from typing import TYPE_CHECKING, Any, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()


class Config(process.ProcessConfig):
    levels: int = Field(
        1,
        description=(
            "How many nesting levels of brackets to strip, counting from 'from_depth' inwards. "
            "0 strips every level from 'from_depth' inwards."
        ),
    )
    from_depth: int = Field(
        0,
        description=(
            "Nesting level to start stripping at, 0 being the outermost substream. Levels above it "
            "are forwarded untouched, so from_depth=1 keeps the outer substream and flattens the "
            "ones nested directly inside it."
        ),
    )
    merge_bracket_attrs: bool = Field(
        True,
        description=(
            "Copy attributes carried by a stripped open-bracket onto every IP that was inside it, "
            "so they are not lost with the bracket. An IP's own attributes win on conflict."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="d4d0606e-211a-4c33-b326-134ff160a60b",
        name="Flatten substreams",
        description=(
            "Remove bracket IPs to flatten substreams, the inverse of 'Wrap IPs into substream'. "
            "Substream sensitive: brackets outside the configured depth range are forwarded unchanged."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="Stream whose substreams should be flattened.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The same IPs, with the brackets of the selected nesting levels removed.",
        ),
    ],
    config=Config,
)


class FlattenSubstreams(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _strips_level(self, level: int) -> bool:
        """Whether the bracket pair at 0-based nesting ``level`` should be removed."""
        if level < self.config.from_depth:
            return False
        if self.config.levels <= 0:
            return True
        return level < self.config.from_depth + self.config.levels

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        # One entry per open substream: whether its brackets are being stripped, and the attributes
        # its open-bracket carried (kept only while stripping, to merge onto the IPs inside).
        open_substreams: list[tuple[bool, dict[str, Any]]] = []
        stripped_brackets = 0

        while self.in_ports["in"] and self.out_ports["out"]:
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                break

            if brackets.is_open_bracket(in_ip):
                strip = self._strips_level(len(open_substreams))
                attrs = brackets.attr_readers(in_ip) if strip and self.config.merge_bracket_attrs else {}
                open_substreams.append((strip, attrs))
                if strip:
                    stripped_brackets += 1
                    continue
            elif brackets.is_close_bracket(in_ip):
                if not open_substreams:
                    logger.warning("%s: close-bracket without a matching open-bracket; forwarding it.", self.name)
                else:
                    strip, _ = open_substreams.pop()
                    if strip:
                        stripped_brackets += 1
                        continue
            elif self.config.merge_bracket_attrs:
                in_ip = self._with_merged_bracket_attrs(in_ip, open_substreams)

            if not await self.write_out("out", in_ip):
                logger.info("%s process finished", self.name)
                return

        if open_substreams:
            logger.warning(
                "%s: input ended with %d substream(s) still open.",
                self.name,
                len(open_substreams),
            )
        logger.info("%s process finished, removed %d bracket IPs", self.name, stripped_brackets)

    @staticmethod
    def _with_merged_bracket_attrs(
        in_ip: IPReader,
        open_substreams: list[tuple[bool, dict[str, Any]]],
    ) -> IPReader:
        """Fold the attributes of every enclosing stripped bracket onto the IP.

        Outer brackets first, so inner ones win, and the IP's own attributes win over all of them.
        """
        inherited: dict[str, Any] = {}
        for _, attrs in open_substreams:
            inherited.update(attrs)
        if not inherited:
            return in_ip

        out_ip = fbp_capnp.IP.new_message(type=in_ip.type)
        out_ip.content = in_ip.content
        merged = dict(inherited)
        merged.update(brackets.attr_readers(in_ip))
        brackets.set_attrs(out_ip, merged)
        return out_ip.as_reader()


def main():
    process.run_process_from_metadata_and_cmd_args(FlattenSubstreams(METADATA), METADATA)


if __name__ == "__main__":
    main()
