#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.merge import merge_relabel_geoparquet_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder

logger = logging.getLogger(__name__)
configure_logging()

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="dfab8c17-56ad-46cc-94a5-c7689ab89b8b",
        name="merge geoparquet",
        description="Merge zipped arrays of relabeled GeoParquet messages.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            type="array",
            contentType="common.capnp:Value[Data]",
            desc="Array of relabeled GeoParquet bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="common.capnp:Value[Data]",
            desc="Merged GeoParquet bytes.",
        ),
    ],
    defaultConfig={
        "code_column": meta.ConfigEntry(
            value="lucode",
            type="string",
            desc="Column containing the land-use/land-cover code.",
        ),
        "priority_column": meta.ConfigEntry(
            value="priority",
            type="string",
            desc="Column containing burn priority.",
        ),
    },
)


class MergeGeoparquetConfig(process.ProcessConfig):
    code_column: str = "lucode"
    priority_column: str = "priority"


class MergeGeoparquet(process.Process[MergeGeoparquetConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while True:
            messages = await self.read_array_in("in", process.ArrayInStrategy.ZIP, automatic_chunking=True)
            if messages is None:
                break

            try:
                parts = [bytes(message.content.as_struct(common_capnp.Value).d) for message in messages]
                output_bytes = merge_relabel_geoparquet_bytes(
                    parts,
                    code_column=self.config.code_column,
                    priority_column=self.config.priority_column,
                )

                if not await self.write_out("out", _data_ip(output_bytes), automatic_chunking=True):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s merged GeoParquet bytes", self.name, len(output_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to merge GeoParquet inputs", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(MergeGeoparquet(METADATA), METADATA)


def _data_ip(data: bytes) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))


if __name__ == "__main__":
    main()
