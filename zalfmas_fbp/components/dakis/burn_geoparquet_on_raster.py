#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.burn import burn_geoparquet_on_raster_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder

logger = logging.getLogger(__name__)
configure_logging()

type BurnRasterCompression = Literal["preserve", "zstd", "deflate", "lzw", "none"]
BURN_RASTER_COMPRESSION_OPTIONS = ["preserve", "zstd", "deflate", "lzw", "none"]

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="18df1635-f2c8-4d86-8b40-622035290c8f",
        name="burn geoparquet on raster",
        description="Burn relabeled GeoParquet geometries into a raster using priority order.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="raster",
            contentType="common.capnp:Value[Data]",
            desc="Compressed raster bytes.",
        ),
        meta.Port(
            name="geometries",
            contentType="common.capnp:Value[Data]",
            desc="GeoParquet bytes with lucode, priority, and geometry columns.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="common.capnp:Value[Data]",
            desc="Burned raster bytes.",
        ),
    ],
    defaultConfig={
        "code_column": meta.ConfigEntry(
            value="lucode",
            type="string",
            desc="Column containing the land-use/land-cover code to burn into the raster.",
        ),
        "priority_column": meta.ConfigEntry(
            value="priority",
            type="string",
            desc="Column used for burn order. Higher values overwrite lower values.",
        ),
        "all_touched": meta.ConfigEntry(
            value=False,
            type="bool",
            desc="Burn every pixel touched by a geometry instead of only pixels whose center is within it.",
        ),
        "compression": meta.ConfigEntry(
            value="preserve",
            type=BURN_RASTER_COMPRESSION_OPTIONS,
            desc="Output raster compression algorithm. Use 'preserve' to keep the incoming raster compression.",
        ),
    },
)


class BurnGeoparquetOnRasterConfig(process.ProcessConfig):
    code_column: str = "lucode"
    priority_column: str = "priority"
    all_touched: bool = False
    compression: BurnRasterCompression = "preserve"


class BurnGeoparquetOnRaster(process.Process[BurnGeoparquetOnRasterConfig]):
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
            raster_msg = await self.read_in("raster", automatic_chunking=True)
            if raster_msg is None:
                break
            geometries_msg = await self.read_in("geometries", automatic_chunking=True)
            if geometries_msg is None:
                break

            try:
                raster_bytes = bytes(raster_msg.content.as_struct(common_capnp.Value).d)
                geoparquet_bytes = bytes(geometries_msg.content.as_struct(common_capnp.Value).d)
                output_bytes = burn_geoparquet_on_raster_bytes(
                    raster_bytes,
                    geoparquet_bytes,
                    code_column=self.config.code_column,
                    priority_column=self.config.priority_column,
                    all_touched=self.config.all_touched,
                    compression=None if self.config.compression == "preserve" else self.config.compression,
                )

                if not await self.write_out("out", _data_ip(output_bytes), automatic_chunking=True):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s burned raster bytes", self.name, len(output_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to burn GeoParquet on raster", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(BurnGeoparquetOnRaster(METADATA), METADATA)


def _data_ip(data: bytes) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))


if __name__ == "__main__":
    main()
