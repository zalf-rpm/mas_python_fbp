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
from typing import Literal, override

from mas.schema.climate import climate_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

DATASET_TYPE = "climate.capnp:Dataset"


class Config(process.ProcessConfig):
    to_attr: str | None = Field(
        default=None,
        description="Send each dataset in this attribute instead of as the IP's content.",
    )
    create_substream: bool = Field(
        default=False,
        description="Wrap each service's datasets in a substream, bracketed by the service's id.",
    )
    on_error: Literal["skip", "fail"] = Field(
        default="skip",
        description="Whether an input that yields no usable service is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(id="climate", name="Climate"),
    info=meta.Info(
        id="79723094-0972-48ec-b219-030dae730063",
        name="climate service -> datasets",
        description=(
            "Send a capability to each dataset available at an incoming climate service. Accepts "
            "either a live capability or a sturdy ref. Substream transparent; can optionally wrap "
            "each service's datasets in a substream of their own."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="cs",
            contentType="climate.capnp:Service",
            desc="Climate service, as a capability or a sturdy ref.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="ds",
            contentType=DATASET_TYPE,
            desc="One IP per dataset of each incoming service.",
            required=True,
        ),
    ],
    config=Config,
)


class ClimateServiceToDatasets(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.sent: int = 0

    async def write_dataset(self, dataset, in_ip) -> bool:
        """One outgoing IP carrying a dataset capability, keeping the input's attributes."""

        out_ip = fbp_capnp.IP.new_message()
        extra = {}
        if self.config.to_attr:
            extra[self.config.to_attr] = brackets.Attr(dataset, DATASET_TYPE)
        else:
            out_ip.content = dataset
            out_ip.sysAttributes.contentType = DATASET_TYPE
        brackets.copy_attrs(in_ip, out_ip, extra=extra)
        self.sent += 1
        return await self.write_out("ds", out_ip)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.in_ports["cs"] and self.out_ports["ds"]:
            in_ip = await self.read_in("cs")
            if in_ip is None:
                self.in_ports["cs"] = None
                break

            if brackets.is_bracket(in_ip):
                if not await self.write_out("ds", in_ip):
                    break
                continue

            service, _ = await self.cast_cap_or_connect(in_ip.content, climate_capnp.Service)
            if service is None:
                message = f"{self.name}: no climate service could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            # Ask for the id while the datasets are being fetched; it is only needed for the
            # brackets, so an unbracketed run never waits on it.
            info_promise = service.info() if self.config.create_substream else None
            datasets = (await service.getAvailableDatasets()).datasets
            if not datasets:
                logger.info("%s: service returned no datasets", self.name)
                continue

            service_id = (await info_promise).id if info_promise is not None else ""
            if self.config.create_substream and not await self.write_out(
                "ds", brackets.make_bracket("openBracket", content=service_id)
            ):
                break

            stopped = False
            for meta_plus_data in datasets:
                if not await self.write_dataset(meta_plus_data.data, in_ip):
                    stopped = True
                    break
            if stopped:
                break

            if self.config.create_substream and not await self.write_out(
                "ds", brackets.make_bracket("closeBracket", content=service_id)
            ):
                break

        logger.info("%s process finished, sent %d dataset(s)", self.name, self.sent)


def main():
    process.run_process_from_metadata_and_cmd_args(ClimateServiceToDatasets(METADATA), METADATA)


if __name__ == "__main__":
    main()
