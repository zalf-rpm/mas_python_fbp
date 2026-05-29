#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    BLOB_CONTENT_TYPE,
    read_prepared_file_metadata,
)
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class WriteFileToDiskConfig(process.ProcessConfig):
    path: str = Field(
        "outputs/dakis",
        description="Fallback output directory if the incoming file payload has no path attribute.",
    )
    filename: str = Field(
        "output.bin",
        description="Fallback filename if the incoming file payload has no filename attribute.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="34f58819-e23f-4cc1-9691-87d5225b122d",
        name="write file to disk",
        description="Write prepared binary file payloads to local disk.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=BLOB_CONTENT_TYPE,
            desc="Prepared file Blob with optional path and filename metadata.",
        ),
    ],
    config=WriteFileToDiskConfig,
)


class WriteFileToDisk(process.Process[WriteFileToDiskConfig]):
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
            stream = await self.read_in_chunked_stream("in")
            if stream is None:
                break

            output_path = await self._write_chunked_file(stream)
            logger.info("%s wrote file to %s", self.name, output_path)

        logger.info("%s process finished", self.name)

    async def _write_chunked_file(self, stream: process.ChunkedInputStream) -> Path:
        path, filename, _content_type = read_prepared_file_metadata(
            stream.open_ip,
            default_path=self.config.path,
            default_filename=self.config.filename,
        )
        output_path = Path(path) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Path | None = None

        try:
            with NamedTemporaryFile(
                "wb", dir=output_path.parent, prefix=f".{output_path.name}.", suffix=".tmp", delete=False
            ) as tmp_file:
                tmp_path = Path(tmp_file.name)
                async for chunk in stream:
                    tmp_file.write(chunk)

            tmp_path.replace(output_path)
        except Exception:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
            raise

        return output_path


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFileToDisk(METADATA), METADATA)


if __name__ == "__main__":
    main()
