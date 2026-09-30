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
from typing import Any, cast, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)


class Config(process.ProcessConfig):
    lift_from_attr: str = Field(
        "attribute_name",
        description="Attribute to read from IP.",
    )
    lift_from_type: str | None = Field(
        None,
        description="@0x123.... = some/path/some_fil._capnp:Type -> Capnp struct type to read from attribute. Not needed if the attributes valueType field is set.",
    )
    lifted_attrs: list[str] = Field(
        ["attr1", "attr2", "attr3"],
        description='["attr1", "attr2", "attr3"] -> Attributes to lift from struct into metadata.',
    )


METADATA = meta.Component(
    category=meta.Category(
        id="ip",
        name="IP (Flow packages)",
    ),
    info=meta.Info(
        id="1ccc2798-23b2-4148-a40f-6b70a69be2fb",
        name="lift attributes",
        description=(
            "Lift fields of a struct-valued attribute into attributes of their own. Substream "
            "transparent: bracket IPs are forwarded unchanged."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="AnyPointer",
            desc="Input message with any content containing some structured attributes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The same content as the message on 'in', but with some attributes lifted out of a structured attribute into the top level attribute metadata.",
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

        lift_from_schema = None
        lift_fieldnames: list[str] = []
        if self.config.lift_from_type is not None:
            lift_from_schema = common.schema_from_content_type_string(self.config.lift_from_type)
            # Only a struct has fields to lift. A content type naming an interface or an enum
            # resolves fine and then has no `as_struct`, which used to raise here.
            as_struct = getattr(lift_from_schema, "as_struct", None)
            if as_struct is None:
                logger.warning(
                    "%s: 'lift_from_type' %r is not a struct, so it has no fields to lift.",
                    self.name,
                    self.config.lift_from_type,
                )
                lift_from_schema = None
            else:
                lift_fieldnames = list(as_struct().fieldnames)

        while self.in_ports["in"] and self.out_ports["out"]:
            # The read is outside the try: a failing read is the runtime's to report, and
            # retrying it here would spin the loop rather than surface the problem.
            in_ip = await self.read_in("in")
            if in_ip is None:
                self.in_ports["in"] = None
                continue

            try:
                # Rebuilding below would drop the IP's type, turning a bracket into a standard IP
                # and destroying the substream, so brackets are forwarded as they are.
                if brackets.is_bracket(in_ip):
                    if not await self.write_out("out", in_ip):
                        logger.info("%s: Could not send IP. Process finished.", self.name)
                        return
                    continue

                # get_fbp_attr defaults `schema` to None but does not say so in its annotation,
                # so passing the None we may hold needs spelling out.
                lift_from_attr = common.get_fbp_attr(in_ip, self.config.lift_from_attr, cast("Any", lift_from_schema))

                out_ip = fbp_capnp.IP.new_message(content=in_ip.content)
                attrs = []
                for attr in in_ip.attributes:
                    attrs.append(attr)

                if lift_from_attr:
                    for l_attr_name in self.config.lifted_attrs:
                        if l_attr_name in lift_fieldnames:
                            attrs.append(
                                {
                                    "key": l_attr_name,
                                    "value": lift_from_attr.__getattribute__(l_attr_name),
                                },
                            )

                out_ip.attributes = attrs  # pyright: ignore
                if not await self.write_out("out", out_ip):
                    logger.info("%s process finished", self.name)
                    return

            except (capnp.KjException, AttributeError):
                # A malformed IP, or an attribute whose struct lacks a configured field.
                logger.exception("%s: could not lift from this IP", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
