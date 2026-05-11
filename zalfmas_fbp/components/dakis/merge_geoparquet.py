#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    GEOPARQUET_CONTENT_TYPE,
    blob_content_type,
)
from zalfmas_fbp.components.dakis.common.merge import merge_relabel_geoparquet_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

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
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="Array of relabeled GeoParquet bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
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
            messages = await self.read_array_in_chunked("in", process.ArrayInStrategy.ZIP)
            if messages is None:
                break

            try:
                parts = [process.read_ip_data(message)[0] for message in messages]
                output_bytes = merge_relabel_geoparquet_bytes(
                    parts,
                    code_column=self.config.code_column,
                    priority_column=self.config.priority_column,
                )

                if not await self.write_out_chunked(
                    "out", process.blob_ip(output_bytes, content_type=GEOPARQUET_CONTENT_TYPE)
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s merged GeoParquet bytes", self.name, len(output_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to merge GeoParquet inputs", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(MergeGeoparquet(METADATA), METADATA)


if __name__ == "__main__":
    main()
