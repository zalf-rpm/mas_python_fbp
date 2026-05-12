#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    BLOB_CONTENT_TYPE,
    DEFAULT_CONTENT_TYPE,
    prepared_file_ip,
)
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
        id="5e4af951-ddeb-4c72-97b4-f2b8bcb60d53",
        name="read file from disk",
        description="Read a binary file from local disk and emit it as a prepared file payload.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="trigger",
            contentType="AnyPointer",
            desc="Optional trigger messages.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=BLOB_CONTENT_TYPE,
            desc="Prepared file Blob with path, filename, and content type metadata.",
        ),
    ],
    defaultConfig={
        "path": meta.ConfigEntry(
            value="outputs/dakis",
            type="string",
            desc="Directory containing the file.",
        ),
        "filename": meta.ConfigEntry(
            value="output.bin",
            type="string",
            desc="Filename to read.",
        ),
        "content_type": meta.ConfigEntry(
            value=DEFAULT_CONTENT_TYPE,
            type="string",
            desc="Content type attribute attached to the outgoing file payload.",
        ),
        "read_once_without_trigger": meta.ConfigEntry(
            value=True,
            type="bool",
            desc="If true, read once when no trigger port is connected.",
        ),
    },
)


class ReadFileFromDiskConfig(process.ProcessConfig):
    path: str = "outputs/dakis"
    filename: str = "output.bin"
    content_type: str = DEFAULT_CONTENT_TYPE
    read_once_without_trigger: bool = True


class ReadFileFromDisk(process.Process[ReadFileFromDiskConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.in_ports.get("trigger") is None and self.config.read_once_without_trigger:
            await self._read_and_send()
            logger.info("%s process finished", self.name)
            return

        while True:
            trigger_msg = await self.read_in("trigger")
            if trigger_msg is None:
                break
            if not await self._read_and_send():
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)

    async def _read_and_send(self) -> bool:
        try:
            file_path = Path(self.config.path) / self.config.filename
            with file_path.open("rb") as file:
                if not await self.write_out_chunked_stream(
                    "out",
                    prepared_file_ip(
                        b"",
                        path=self.config.path,
                        filename=self.config.filename,
                        content_type=self.config.content_type,
                    ),
                    chunks=_file_chunks(file),
                ):
                    return False

        except OSError:
            logger.exception("%s failed to read file from disk", self.name)
            return True
        else:
            logger.info("%s read file from %s", self.name, file_path)
            return True


def main():
    process.run_process_from_metadata_and_cmd_args(ReadFileFromDisk(METADATA), METADATA)


if __name__ == "__main__":
    main()


async def _file_chunks(file):
    while chunk := await asyncio.to_thread(file.read, process.DEFAULT_BRACKETED_CHUNK_SIZE):
        yield bytes(chunk)
