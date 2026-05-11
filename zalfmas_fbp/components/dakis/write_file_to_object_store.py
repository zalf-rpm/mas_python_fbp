#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from tempfile import TemporaryFile
from typing import BinaryIO, override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    BLOB_CONTENT_TYPE,
    read_prepared_file_metadata,
)
from zalfmas_fbp.components.dakis.common.object_store import object_store_bucket_and_key, put_object
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
        id="28c6182f-0a3a-4afd-b3e1-856c796f1b2a",
        name="write file to object store",
        description="Write prepared binary file payloads to an S3-compatible object store.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=BLOB_CONTENT_TYPE,
            desc="Prepared file Blob with optional path and filename metadata.",
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
            desc="Fallback object key prefix if the incoming file payload has no path attribute.",
        ),
        "filename": meta.ConfigEntry(
            value="output.bin",
            type="string",
            desc="Fallback filename if the incoming file payload has no filename attribute.",
        ),
    },
)


class WriteFileToObjectStoreConfig(process.ProcessConfig):
    object_store_url: str = "https://objects.dakispro.de"
    access_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    path: str = "dakis"
    filename: str = "output.bin"


class WriteFileToObjectStore(process.Process[WriteFileToObjectStoreConfig]):
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

            path, filename, content_type, body = await self._chunked_body(stream)
            bucket, key = _bucket_and_key(
                bucket=self.config.bucket,
                path=path,
                filename=filename,
            )
            put_object(
                endpoint_url=self.config.object_store_url,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket=bucket,
                key=key,
                body=body,
                content_type=content_type,
            )
            if not isinstance(body, bytes):
                body.close()
            logger.info("%s wrote file to object store at s3://%s/%s", self.name, bucket, key)

        logger.info("%s process finished", self.name)

    async def _chunked_body(self, stream: process.ChunkedInputStream) -> tuple[str, str, str, BinaryIO]:
        path, filename, content_type = read_prepared_file_metadata(
            stream.open_ip,
            default_path=self.config.path,
            default_filename=self.config.filename,
        )
        tmp_file: BinaryIO = TemporaryFile("w+b")
        async for chunk in stream:
            tmp_file.write(chunk)
        tmp_file.seek(0)
        return path, filename, content_type, tmp_file


def _bucket_and_key(*, bucket: str, path: str, filename: str) -> tuple[str, str]:
    return object_store_bucket_and_key(bucket=bucket, path=path, filename=filename)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFileToObjectStore(METADATA), METADATA)


if __name__ == "__main__":
    main()
