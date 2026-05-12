from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from ..errors import InputPortReadError

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader


DEFAULT_BRACKETED_CHUNK_SIZE = 16 * 1024 * 1024


def ip_content_type(ip: IPBuilder | IPReader) -> str | None:
    try:
        return ip.sysAttributes.contentType or None
    except (capnp.KjException, AttributeError, TypeError):
        return None


def ip_blob_payload(ip: IPBuilder | IPReader) -> tuple[bytes, str | None]:
    content_type = ip_content_type(ip)
    blob = ip.content.as_struct(common_capnp.Blob)
    return bytes(blob.data), blob.contentType or content_type


def blob_ip(data: bytes, *, content_type: str) -> IPBuilder:
    return fbp_capnp.IP.new_message(
        content=common_capnp.Blob.new_message(contentType=content_type, data=data),
        sysAttributes={"contentType": content_type},
    )


def read_ip_data(
    ip: IPBuilder | IPReader,
    *,
    default_content_type: str = common_capnp.MimeTypes.applicationOctetStream,
) -> tuple[bytes, str]:
    data, content_type = ip_blob_payload(ip)
    return data, content_type or default_content_type


def iter_bytes_in_chunks(
    data: bytes,
    *,
    chunk_size: int | None = None,
) -> Iterable[bytes]:
    chunk_size = chunk_size or DEFAULT_BRACKETED_CHUNK_SIZE
    for offset in range(0, len(data), chunk_size):
        yield data[offset : offset + chunk_size]


def chunked_blob_ip(
    data: bytes,
    *,
    content_type: str | None,
    source: IPBuilder | IPReader | None = None,
) -> IPBuilder:
    resolved_content_type = content_type or common_capnp.MimeTypes.applicationOctetStream
    out_ip = blob_ip(data, content_type=resolved_content_type)
    if source is not None:
        out_ip.attributes = list(source.attributes)
    return out_ip


def bracket_ip(
    bracket_type: Literal["openBracket", "closeBracket"],
    source: IPBuilder | IPReader,
    *,
    content_type: str | None = None,
    chunk_count: int = 0,
) -> IPBuilder:
    sys_attributes: dict[str, Any] = {"bracketType": {"chunkedContent": {"chunkCount": chunk_count}}}
    if content_type is not None:
        sys_attributes["contentType"] = content_type
    bracketed_ip = fbp_capnp.IP.new_message(type=bracket_type, sysAttributes=sys_attributes)
    if bracket_type == "openBracket":
        bracketed_ip.attributes = list(source.attributes)
    return bracketed_ip


@dataclass
class ChunkedInputStream:
    open_ip: IPReader | IPBuilder
    process_name: str | None
    port: str
    _read_next_ip: Callable[[], Awaitable[IPReader | None]]
    _is_stopping: Callable[[], bool]
    _on_complete: Callable[[], Awaitable[None]] | None = None
    _single_chunk: bytes | None = None
    _done: bool = False
    content_type: str | None = None

    def __aiter__(self) -> ChunkedInputStream:
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration

        if self._single_chunk is not None:
            chunk = self._single_chunk
            self._single_chunk = None
            self._done = True
            return chunk

        chunk_ip = await self._read_next_ip()
        if chunk_ip is None:
            self._done = True
            if self._is_stopping():
                raise StopAsyncIteration
            msg = f"{self.process_name} input port '{self.port}' closed before a bracketed payload ended."
            raise InputPortReadError(self.process_name, self.port, msg)

        if chunk_ip.type == "closeBracket":
            self._done = True
            if self._on_complete is not None:
                await self._on_complete()
            raise StopAsyncIteration

        if chunk_ip.type == "openBracket":
            self._done = True
            msg = f"{self.process_name} received a nested openBracket on input port '{self.port}'."
            raise InputPortReadError(self.process_name, self.port, msg)

        try:
            chunk, chunk_content_type = ip_blob_payload(chunk_ip)
        except (capnp.KjException, TypeError) as e:
            msg = f"{self.process_name} can only read common.capnp:Blob chunked payloads on input port '{self.port}'."
            raise InputPortReadError(self.process_name, self.port, msg) from e

        if self.content_type is None:
            self.content_type = chunk_content_type
        return chunk

    async def collect_blob(self) -> IPBuilder:
        chunks: list[bytes] = []
        async for chunk in self:
            chunks.append(chunk)
        return chunked_blob_ip(
            b"".join(chunks),
            content_type=self.content_type,
            source=self.open_ip,
        )
