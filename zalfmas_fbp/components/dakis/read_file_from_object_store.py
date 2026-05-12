#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import asyncio
import logging
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    BLOB_CONTENT_TYPE,
    DEFAULT_CONTENT_TYPE,
    prepared_file_ip,
)
from zalfmas_fbp.components.dakis.common.object_store import (
    get_object_body,
    object_store_bucket_and_key,
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
        id="29aa4f52-3e62-4f03-9d05-d2b82d611b71",
        name="read file from object store",
        description="Read a binary file from an S3-compatible object store and emit it as a prepared file payload.",
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
        "object_store_url": meta.ConfigEntry(
            value="https://objects.dakispro.de",
            type="string",
            desc="S3-compatible object store endpoint URL.",
        ),
        "access_key": meta.ConfigEntry(
            value="",
            type="string",
            desc="Object store access key.",
        ),
        "secret_key": meta.ConfigEntry(
            value="",
            type="string",
            desc="Object store secret key.",
        ),
        "bucket": meta.ConfigEntry(
            value="",
            type="string",
            desc="Object store bucket. If empty, the first path segment is used as the bucket.",
        ),
        "path": meta.ConfigEntry(
            value="dakis",
            type="string",
            desc="Object key prefix or bucket/prefix when bucket is empty.",
        ),
        "filename": meta.ConfigEntry(
            value="output.bin",
            type="string",
            desc="Object filename to read.",
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


class ReadFileFromObjectStoreConfig(process.ProcessConfig):
    object_store_url: str = "https://objects.dakispro.de"
    access_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    path: str = "dakis"
    filename: str = "output.bin"
    content_type: str = DEFAULT_CONTENT_TYPE
    read_once_without_trigger: bool = True


class ReadFileFromObjectStore(process.Process[ReadFileFromObjectStoreConfig]):
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
            bucket, key = _bucket_and_key(
                bucket=self.config.bucket, path=self.config.path, filename=self.config.filename
            )
            body = get_object_body(
                endpoint_url=self.config.object_store_url,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket=bucket,
                key=key,
            )
            try:
                if not await self.write_out_chunked_stream(
                    "out",
                    prepared_file_ip(
                        b"",
                        path=self.config.path,
                        filename=self.config.filename,
                        content_type=self.config.content_type,
                    ),
                    chunks=_body_chunks(body),
                ):
                    return False
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()

        except (ImportError, TypeError, ValueError):
            logger.exception("%s failed to read file from object store", self.name)
            return True
        else:
            logger.info("%s read file from object store at s3://%s/%s", self.name, bucket, key)
            return True


def _bucket_and_key(*, bucket: str, path: str, filename: str) -> tuple[str, str]:
    return object_store_bucket_and_key(bucket=bucket, path=path, filename=filename)


def main():
    process.run_process_from_metadata_and_cmd_args(ReadFileFromObjectStore(METADATA), METADATA)


if __name__ == "__main__":
    main()


async def _body_chunks(body):
    while chunk := await asyncio.to_thread(body.read, process.DEFAULT_BRACKETED_CHUNK_SIZE):
        yield bytes(chunk)
