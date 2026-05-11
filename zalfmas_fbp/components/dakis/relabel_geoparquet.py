#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, override

import capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    CSV_CONTENT_TYPE,
    GEOPARQUET_CONTENT_TYPE,
    blob_content_type,
)
from zalfmas_fbp.components.dakis.common.relabel import relabel_geoparquet_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="9db20ca7-ba92-4179-8f54-8e9945893421",
        name="relabel geoparquet",
        description="Relabel GeoParquet geometries using a CSV mapping table.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="GeoParquet bytes.",
        ),
        meta.Port(
            name="translation",
            contentType=f"Text | {blob_content_type(CSV_CONTENT_TYPE)}",
            desc="Optional CSV mapping table bytes or path. If unconnected, mapping_csv_path is used.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="Relabeled GeoParquet bytes.",
        ),
    ],
    defaultConfig={
        "mapping_csv_path": meta.ConfigEntry(
            value="resources/mappings/invekos_to_lulc.csv",
            type="string",
            desc="Path to the CSV mapping table.",
        ),
        "source_code_column": meta.ConfigEntry(
            value="code",
            type="string",
            desc="Column in the GeoParquet and mapping CSV containing the source code.",
        ),
        "target_code_column": meta.ConfigEntry(
            value="lucode",
            type="string",
            desc="Column in the mapping CSV to write to the output GeoParquet.",
        ),
        "priority_column": meta.ConfigEntry(
            value="priority",
            type="string",
            desc="Optional priority column in the mapping CSV and output GeoParquet.",
        ),
        "default_priority": meta.ConfigEntry(
            value=0,
            type="int",
            desc="Priority value used when the mapping CSV has no priority column or value.",
        ),
    },
)


class RelabelGeoparquetConfig(process.ProcessConfig):
    mapping_csv_path: str = "resources/mappings/invekos_to_lulc.csv"
    source_code_column: str = "code"
    target_code_column: str = "lucode"
    priority_column: str = "priority"
    default_priority: int = 0


class RelabelGeoparquet(process.Process[RelabelGeoparquetConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        mapping_csv_path = self.config.mapping_csv_path
        mapping_csv_bytes = None

        while True:
            in_msg = await self.read_in_chunked("in")
            if in_msg is None:
                break

            try:
                translation_msg = await self.read_in_chunked("translation")
                if translation_msg is not None:
                    mapping_csv_path, mapping_csv_bytes = _read_translation(
                        translation_msg,
                        fallback_path=mapping_csv_path,
                    )

                geoparquet_bytes, _content_type = process.read_ip_data(in_msg)
                output_bytes = relabel_geoparquet_bytes(
                    geoparquet_bytes,
                    mapping_csv_path=mapping_csv_path,
                    mapping_csv_bytes=mapping_csv_bytes,
                    source_code_column=self.config.source_code_column,
                    target_code_column=self.config.target_code_column,
                    priority_column=self.config.priority_column,
                    default_priority=self.config.default_priority,
                )

                if not await self.write_out_chunked(
                    "out", process.blob_ip(output_bytes, content_type=GEOPARQUET_CONTENT_TYPE)
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s relabeled GeoParquet bytes", self.name, len(output_bytes))

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break
            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to relabel GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(RelabelGeoparquet(METADATA), METADATA)


def _read_translation(ip: IPReader | IPBuilder, *, fallback_path: str) -> tuple[str, bytes | None]:
    try:
        data, _content_type = process.read_ip_data(ip, default_content_type=CSV_CONTENT_TYPE)
        return fallback_path, data
    except (capnp.KjException, TypeError):
        return ip.content.as_text() or fallback_path, None


if __name__ == "__main__":
    main()
